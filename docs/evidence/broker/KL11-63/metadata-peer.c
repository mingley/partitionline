/* Actual pinned librdkafka Admin/Metadata runtime peer. No Rust API linked. */
#include "rdkafka.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static int checks;
static void require(int condition, const char *label) {
    checks++;
    if (!condition) { fprintf(stderr, "assertion failed: %s\n", label); exit(1); }
}
static void set(rd_kafka_conf_t *conf, const char *name, const char *value) {
    char error[512];
    require(rd_kafka_conf_set(conf, name, value, error, sizeof(error)) == RD_KAFKA_CONF_OK, name);
}
static void metadata(rd_kafka_t *client, const char *name, int expected) {
    const struct rd_kafka_metadata *result = NULL;
    require(rd_kafka_metadata(client, 1, NULL, &result, 10000) == RD_KAFKA_RESP_ERR_NO_ERROR, "all-topics metadata request");
    require(result != NULL && result->broker_cnt == 1 && result->brokers[0].id == 0, "single-node metadata");
    int found = 0;
    for (int i = 0; i < result->topic_cnt; i++) {
        if (strcmp(result->topics[i].topic, name) == 0) {
            if (!expected) {
                require(result->topics[i].err == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART, "missing topic error");
                continue;
            }
            found = 1;
            require(result->topics[i].err == RD_KAFKA_RESP_ERR_NO_ERROR, "topic metadata error");
            require(result->topics[i].partition_cnt == 2, "partition count");
            for (int p = 0; p < result->topics[i].partition_cnt; p++) {
                require(result->topics[i].partitions[p].leader == 0, "leader node0");
            }
        }
    }
    printf("{\"operation\":\"metadata\",\"name\":\"%s\",\"found\":%s,\"topics\":%d}\n", name, found ? "true" : "false", result->topic_cnt);
    require(found == expected, "topic existence");
    rd_kafka_metadata_destroy(result);
}
static void create(rd_kafka_t *client, rd_kafka_queue_t *queue, const char *name, int validate, rd_kafka_resp_err_t expected) {
    char error[512];
    rd_kafka_NewTopic_t *topic = rd_kafka_NewTopic_new(name, 2, 1, error, sizeof(error));
    require(topic != NULL, "new topic allocation");
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(client, RD_KAFKA_ADMIN_OP_CREATETOPICS);
    require(options != NULL, "admin options allocation");
    require(rd_kafka_AdminOptions_set_request_timeout(options, 10000, error, sizeof(error)) == RD_KAFKA_RESP_ERR_NO_ERROR, "request timeout");
    require(rd_kafka_AdminOptions_set_validate_only(options, validate, error, sizeof(error)) == RD_KAFKA_RESP_ERR_NO_ERROR, "validate-only option");
    rd_kafka_CreateTopics(client, &topic, 1, options, queue);
    rd_kafka_event_t *event = rd_kafka_queue_poll(queue, 15000);
    require(event != NULL && rd_kafka_event_type(event) == RD_KAFKA_EVENT_CREATETOPICS_RESULT, "create result event");
    require(rd_kafka_event_error(event) == RD_KAFKA_RESP_ERR_NO_ERROR, "create event error");
    size_t count = 0;
    const rd_kafka_topic_result_t **results = rd_kafka_CreateTopics_result_topics(rd_kafka_event_CreateTopics_result(event), &count);
    require(count == 1, "one create result");
    rd_kafka_resp_err_t actual = rd_kafka_topic_result_error(results[0]);
    printf("{\"operation\":\"create\",\"name\":\"%s\",\"validate_only\":%s,\"error\":%d}\n", name, validate ? "true" : "false", (int) actual);
    require(actual == expected, "create expected status");
    rd_kafka_event_destroy(event); rd_kafka_AdminOptions_destroy(options); rd_kafka_NewTopic_destroy(topic);
}
static void delete_topic(rd_kafka_t *client, rd_kafka_queue_t *queue, const char *name, rd_kafka_resp_err_t expected) {
    rd_kafka_DeleteTopic_t *topic = rd_kafka_DeleteTopic_new(name);
    require(topic != NULL, "delete topic allocation");
    rd_kafka_DeleteTopics(client, &topic, 1, NULL, queue);
    rd_kafka_event_t *event = rd_kafka_queue_poll(queue, 15000);
    require(event != NULL && rd_kafka_event_type(event) == RD_KAFKA_EVENT_DELETETOPICS_RESULT, "delete result event");
    require(rd_kafka_event_error(event) == RD_KAFKA_RESP_ERR_NO_ERROR, "delete event error");
    size_t count = 0;
    const rd_kafka_topic_result_t **results = rd_kafka_DeleteTopics_result_topics(rd_kafka_event_DeleteTopics_result(event), &count);
    require(count == 1, "one delete result");
    rd_kafka_resp_err_t actual = rd_kafka_topic_result_error(results[0]);
    printf("{\"operation\":\"delete\",\"name\":\"%s\",\"error\":%d}\n", name, (int) actual);
    require(actual == expected, "delete expected status");
    rd_kafka_event_destroy(event); rd_kafka_DeleteTopic_destroy(topic);
}
int main(int argc, char **argv) {
    require(argc == 3, "broker create|restart arguments");
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    set(conf, "bootstrap.servers", argv[1]); set(conf, "client.id", "metadata-c-peer");
    set(conf, "socket.timeout.ms", "10000"); set(conf, "allow.auto.create.topics", "false");
    set(conf, "debug", "protocol,admin");
    char error[512];
    rd_kafka_t *client = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
    require(client != NULL, "client allocation");
    rd_kafka_queue_t *queue = rd_kafka_queue_new(client); require(queue != NULL, "queue allocation");
    const char *name = "peer-c-persistent";
    printf("{\"peer\":\"librdkafka\",\"version\":\"%s\",\"phase\":\"%s\"}\n", rd_kafka_version_str(), argv[2]);
    if (strcmp(argv[2], "all-probe") == 0) {
        const struct rd_kafka_metadata *result = NULL;
        rd_kafka_resp_err_t error_code = rd_kafka_metadata(client, 1, NULL, &result, 10000);
        printf("{\"operation\":\"all-topics-diagnostic-only\",\"error\":%d}\n", (int) error_code);
        if (result != NULL) rd_kafka_metadata_destroy(result);
        rd_kafka_queue_destroy(queue); rd_kafka_destroy(client);
        printf("{\"status\":\"diagnostic-only\"}\n");
        return 0;
    } else if (strcmp(argv[2], "create") == 0) {
        create(client, queue, "peer-c-validate", 1, RD_KAFKA_RESP_ERR_NO_ERROR);
        metadata(client, "peer-c-validate", 0);
        create(client, queue, name, 0, RD_KAFKA_RESP_ERR_NO_ERROR); metadata(client, name, 1);
        create(client, queue, name, 0, RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS);
    } else {
        require(strcmp(argv[2], "restart") == 0, "restart phase");
        metadata(client, name, 1); delete_topic(client, queue, name, RD_KAFKA_RESP_ERR_NO_ERROR);
        metadata(client, name, 0); delete_topic(client, queue, "peer-c-missing", RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART);
    }
    rd_kafka_queue_destroy(queue); rd_kafka_destroy(client);
    printf("{\"status\":\"pass\",\"assertions\":%d}\n", checks);
    return 0;
}
