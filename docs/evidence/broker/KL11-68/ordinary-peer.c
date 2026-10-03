/* Actual pinned librdkafka ordinary Produce/manual Fetch/ListOffsets peer. */
#define _POSIX_C_SOURCE 200809L
#include "rdkafka.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_RECORD_COUNT 13
#define RECORD_TIME INT64_C(1700000000000)
static int checks;
static int deliveries;
static int record_count = 12;
static const char *topic_name = "ordinary-native";
typedef struct receipt_s { int partition; int index; int delivered; char id[96]; char key[128]; char value[128]; } receipt_t;
static receipt_t receipts[2][MAX_RECORD_COUNT];

static void require(int condition, const char *label) {
    checks++;
    if (!condition) { fprintf(stderr, "assertion failed: %s\n", label); exit(1); }
}
static void set(rd_kafka_conf_t *conf, const char *key, const char *value) {
    char error[512]; require(rd_kafka_conf_set(conf, key, value, error, sizeof(error)) == RD_KAFKA_CONF_OK, key);
}
static void prepare(void) {
    for (int p = 0; p < 2; p++) for (int i = 0; i < record_count; i++) {
        receipt_t *r = &receipts[p][i]; r->partition = p; r->index = i;
        require(snprintf(r->id, sizeof(r->id), "%s:%d:%d", topic_name, p, i) > 0, "receipt id");
        require(snprintf(r->key, sizeof(r->key), "key:%s:%d:%d", topic_name, p, i) > 0, "receipt key");
        require(snprintf(r->value, sizeof(r->value), "value:%s:%d:%d", topic_name, p, i) > 0, "receipt value");
    }
}
static void print_hex(const void *bytes, size_t length) {
    if (bytes == NULL) { fputs("null", stdout); return; }
    const unsigned char *data = bytes; putchar('"');
    for (size_t i = 0; i < length; i++) printf("%02x", data[i]);
    putchar('"');
}
static void print_receipt(const char *operation, const receipt_t *r, const void *key, size_t key_length,
                          const void *value, size_t value_length, int64_t offset, int64_t timestamp) {
    printf("{\"operation\":\"%s\",\"id\":\"%s\",\"topic\":\"%s\",\"partition\":%d,\"offset\":%" PRId64 ",\"timestamp\":%" PRId64 ",\"key_hex\":",
           operation, r->id, topic_name, r->partition, offset, timestamp);
    print_hex(key, key_length); fputs(",\"value_hex\":", stdout); print_hex(value, value_length);
    fputs(",\"headers\":[{\"key\":\"receipt\",\"value_hex\":", stdout); print_hex(r->id, strlen(r->id));
    fputs("},{\"key\":\"dup\",\"value_hex\":\"61\"},{\"key\":\"dup\",\"value_hex\":null}]}\n", stdout); fflush(stdout);
}
static void delivery(rd_kafka_t *client, const rd_kafka_message_t *message, void *opaque) {
    (void)client; (void)opaque; receipt_t *r = message->_private;
    require(r != NULL && !r->delivered, "unique callback identity");
    require(message->err == RD_KAFKA_RESP_ERR_NO_ERROR, "successful real Producer delivery");
    require(message->partition == r->partition && message->offset == r->index, "real Producer assigned offset/partition");
    r->delivered = 1; deliveries++;
    const void *key = r->index % 3 == 0 ? NULL : r->key;
    const void *value = r->index % 4 == 0 ? NULL : r->index % 4 == 1 ? "" : r->value;
    print_receipt("delivery", r, key, key == NULL ? 0 : strlen(r->key), value,
                  value == NULL ? 0 : strlen(value), message->offset, RECORD_TIME + r->partition * 100 + r->index);
}
static rd_kafka_t *new_client(const char *bootstrap, int consumer) {
    rd_kafka_conf_t *conf = rd_kafka_conf_new(); set(conf, "bootstrap.servers", bootstrap);
    set(conf, "client.id", "ordinary-native-peer"); set(conf, "allow.auto.create.topics", "false");
    set(conf, "socket.timeout.ms", "10000"); set(conf, "debug", "protocol,feature,broker");
    if (consumer) {
        set(conf, "group.id", "ordinary-native-manual"); set(conf, "enable.auto.commit", "false");
        set(conf, "enable.auto.offset.store", "false"); set(conf, "auto.offset.reset", "error");
        set(conf, "fetch.min.bytes", "1"); set(conf, "fetch.wait.max.ms", "20");
        set(conf, "message.max.bytes", "65536");
        set(conf, "fetch.max.bytes", "65536"); set(conf, "fetch.message.max.bytes", "4096");
        set(conf, "isolation.level", "read_uncommitted");
    } else {
        set(conf, "enable.idempotence", "false"); set(conf, "acks", "1");
        set(conf, "message.send.max.retries", "0"); set(conf, "max.in.flight.requests.per.connection", "1");
        set(conf, "compression.type", "none"); set(conf, "linger.ms", "5");
        set(conf, "message.timeout.ms", "15000"); set(conf, "request.timeout.ms", "5000");
        set(conf, "queue.buffering.max.kbytes", "1024"); rd_kafka_conf_set_dr_msg_cb(conf, delivery);
    }
    char error[512]; rd_kafka_t *client = rd_kafka_new(consumer ? RD_KAFKA_CONSUMER : RD_KAFKA_PRODUCER, conf, error, sizeof(error));
    if (client == NULL) fprintf(stderr, "new client failed: %s\n", error);
    require(client != NULL, "client allocation"); return client;
}
static void create_topic(rd_kafka_t *client) {
    char error[512]; rd_kafka_NewTopic_t *topic = rd_kafka_NewTopic_new(topic_name, 2, 1, error, sizeof(error));
    require(topic != NULL, "new topic allocation"); rd_kafka_queue_t *queue = rd_kafka_queue_new(client);
    require(queue != NULL, "admin queue allocation"); rd_kafka_CreateTopics(client, &topic, 1, NULL, queue);
    rd_kafka_event_t *event = rd_kafka_queue_poll(queue, 15000);
    require(event != NULL && rd_kafka_event_type(event) == RD_KAFKA_EVENT_CREATETOPICS_RESULT, "actual CreateTopics result");
    require(rd_kafka_event_error(event) == RD_KAFKA_RESP_ERR_NO_ERROR, "CreateTopics event success");
    size_t count = 0; const rd_kafka_topic_result_t **topics = rd_kafka_CreateTopics_result_topics(rd_kafka_event_CreateTopics_result(event), &count);
    require(count == 1 && rd_kafka_topic_result_error(topics[0]) == RD_KAFKA_RESP_ERR_NO_ERROR, "actual topic creation");
    rd_kafka_event_destroy(event); rd_kafka_queue_destroy(queue); rd_kafka_NewTopic_destroy(topic);
}
static void all_metadata(rd_kafka_t *client) {
    const struct rd_kafka_metadata *metadata = NULL;
    require(rd_kafka_metadata(client, 1, NULL, &metadata, 10000) == RD_KAFKA_RESP_ERR_NO_ERROR, "unchanged actual native all-topics query");
    require(metadata != NULL && metadata->broker_cnt == 1 && metadata->brokers[0].id == 0, "metadata node0");
    int found = 0;
    for (int i = 0; i < metadata->topic_cnt; i++) if (strcmp(metadata->topics[i].topic, topic_name) == 0) {
        require(metadata->topics[i].err == RD_KAFKA_RESP_ERR_NO_ERROR && metadata->topics[i].partition_cnt == 2, "native topic metadata"); found++;
    }
    require(found == 1, "native topic present once"); rd_kafka_metadata_destroy(metadata);
}
static void append(const char *bootstrap) {
    rd_kafka_t *client = new_client(bootstrap, 0); create_topic(client); all_metadata(client);
    for (int p = 0; p < 2; p++) for (int i = 0; i < record_count; i++) {
        receipt_t *r = &receipts[p][i]; rd_kafka_headers_t *headers = rd_kafka_headers_new(3);
        require(headers != NULL, "headers allocation");
        require(rd_kafka_header_add(headers, "receipt", -1, r->id, (ssize_t)strlen(r->id)) == RD_KAFKA_RESP_ERR_NO_ERROR, "receipt header");
        require(rd_kafka_header_add(headers, "dup", -1, "a", 1) == RD_KAFKA_RESP_ERR_NO_ERROR, "first duplicate header");
        require(rd_kafka_header_add(headers, "dup", -1, NULL, 0) == RD_KAFKA_RESP_ERR_NO_ERROR, "null duplicate header");
        void *key = i % 3 == 0 ? NULL : r->key; void *value = i % 4 == 0 ? NULL : i % 4 == 1 ? (void *)"" : r->value;
        size_t key_length = key == NULL ? 0 : strlen(r->key);
        size_t value_length = value == NULL ? 0 : strlen(value);
        rd_kafka_resp_err_t error = rd_kafka_producev(client, RD_KAFKA_V_TOPIC(topic_name), RD_KAFKA_V_PARTITION(p),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_KEY(key, key_length),
            RD_KAFKA_V_VALUE(value, value_length), RD_KAFKA_V_HEADERS(headers),
            RD_KAFKA_V_TIMESTAMP(RECORD_TIME + p * 100 + i), RD_KAFKA_V_OPAQUE(r), RD_KAFKA_V_END);
        if (error != RD_KAFKA_RESP_ERR_NO_ERROR) rd_kafka_headers_destroy(headers);
        require(error == RD_KAFKA_RESP_ERR_NO_ERROR, "real Produce enqueue");
    }
    require(rd_kafka_flush(client, 15000) == RD_KAFKA_RESP_ERR_NO_ERROR, "real Producer flush");
    require(deliveries == 2 * record_count, "every real Producer delivery observed"); rd_kafka_destroy(client);
}
static int64_t monotonic_ms(void) {
    struct timespec value; require(clock_gettime(CLOCK_MONOTONIC, &value) == 0, "clock read");
    return (int64_t)value.tv_sec * 1000 + value.tv_nsec / 1000000;
}
static void same(const void *actual, size_t length, const void *expected, const char *label) {
    require((actual == NULL) == (expected == NULL), label);
    require(length == (expected == NULL ? 0 : strlen(expected)), label);
    if (length > 0) require(memcmp(actual, expected, length) == 0, label);
}
static void fetch(const char *bootstrap) {
    rd_kafka_t *client = new_client(bootstrap, 1); all_metadata(client);
    require(rd_kafka_poll_set_consumer(client) == RD_KAFKA_RESP_ERR_NO_ERROR, "consumer queue");
    rd_kafka_topic_partition_list_t *assignment = rd_kafka_topic_partition_list_new(2);
    require(assignment != NULL, "assignment allocation");
    for (int p = 0; p < 2; p++) rd_kafka_topic_partition_list_add(assignment, topic_name, p)->offset = 0;
    require(rd_kafka_assign(client, assignment) == RD_KAFKA_RESP_ERR_NO_ERROR, "actual manual assignment");
    int next[2] = {0, 0}; int received = 0; int64_t deadline = monotonic_ms() + 20000;
    while (received < 2 * record_count && monotonic_ms() < deadline) {
        rd_kafka_message_t *message = rd_kafka_consumer_poll(client, 100);
        if (message == NULL) continue;
        if (message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) { rd_kafka_message_destroy(message); continue; }
        require(message->err == RD_KAFKA_RESP_ERR_NO_ERROR, "actual Fetch message error");
        require(message->partition >= 0 && message->partition < 2, "actual Fetch partition bound");
        int partition = message->partition; require(next[partition] < record_count && message->offset == next[partition], "actual Fetch offset/order/no duplicates");
        receipt_t *r = &receipts[partition][next[partition]];
        const void *key = r->index % 3 == 0 ? NULL : r->key;
        const void *value = r->index % 4 == 0 ? NULL : r->index % 4 == 1 ? "" : r->value;
        same(message->key, message->key_len, key, "actual key bytes/null/empty"); same(message->payload, message->len, value, "actual value bytes/null/empty");
        rd_kafka_timestamp_type_t type; int64_t timestamp = rd_kafka_message_timestamp(message, &type);
        require(type == RD_KAFKA_TIMESTAMP_CREATE_TIME && timestamp == RECORD_TIME + partition * 100 + r->index, "actual CreateTime timestamp");
        rd_kafka_headers_t *headers = NULL; require(rd_kafka_message_headers(message, &headers) == RD_KAFKA_RESP_ERR_NO_ERROR, "actual record headers");
        require(rd_kafka_header_cnt(headers) == 3, "actual duplicate header count");
        for (size_t h = 0; h < 3; h++) {
            const char *name = NULL; const void *bytes = NULL; size_t length = 0;
            require(rd_kafka_header_get_all(headers, h, &name, &bytes, &length) == RD_KAFKA_RESP_ERR_NO_ERROR, "actual header index");
            require(strcmp(name, h == 0 ? "receipt" : "dup") == 0, "actual header order/key");
            same(bytes, length, h == 0 ? r->id : h == 1 ? "a" : NULL, "actual header value/null");
        }
        print_receipt("consume", r, message->key, message->key_len, message->payload, message->len, message->offset, timestamp);
        next[partition]++; received++; rd_kafka_message_destroy(message);
    }
    require(received == 2 * record_count, "every actual record received");
    for (int p = 0; p < 2; p++) {
        int64_t first, last; require(rd_kafka_query_watermark_offsets(client, topic_name, p, &first, &last, 10000) == RD_KAFKA_RESP_ERR_NO_ERROR, "actual ListOffsets watermarks");
        require(first == 0 && last == record_count, "actual retained start/end offsets");
    }
    for (int p = 0; p < 2; p++) assignment->elems[p].offset = RECORD_TIME + p * 100 + 5;
    require(rd_kafka_offsets_for_times(client, assignment, 10000) == RD_KAFKA_RESP_ERR_NO_ERROR, "actual timestamp ListOffsets");
    for (int p = 0; p < 2; p++) require(assignment->elems[p].err == RD_KAFKA_RESP_ERR_NO_ERROR && assignment->elems[p].offset == 5, "actual timestamp offset5");
    require(rd_kafka_consumer_close(client) == RD_KAFKA_RESP_ERR_NO_ERROR, "manual consumer close");
    rd_kafka_topic_partition_list_destroy(assignment); rd_kafka_destroy(client);
}
int main(int argc, char **argv) {
    require(argc == 3 || argc == 5, "bootstrap append|fetch|restart [SDK-topic count12or13]");
    if (argc == 5) {
        require(strcmp(argv[2], "append") != 0, "foreign SDK topics are read-only");
        topic_name = argv[3]; require(strlen(topic_name) > 0 && strlen(topic_name) <= 64, "bounded topic name");
        for (size_t i = 0; topic_name[i] != '\0'; i++) {
            char c = topic_name[i];
            require((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.', "ordinary topic name");
        }
        char *end = NULL; long count = strtol(argv[4], &end, 10);
        require(end != argv[4] && *end == '\0' && (count == 12 || count == 13), "bounded SDK receipt count");
        record_count = (int)count;
    }
    prepare();
    printf("{\"peer\":\"librdkafka\",\"version\":\"%s\",\"phase\":\"%s\",\"ordinary\":true}\n", rd_kafka_version_str(), argv[2]);
    if (strcmp(argv[2], "append") == 0) append(argv[1]);
    else { require(strcmp(argv[2], "fetch") == 0 || strcmp(argv[2], "restart") == 0, "fetch phase"); fetch(argv[1]); }
    printf("{\"status\":\"pass\",\"assertions\":%d,\"records\":%d}\n", checks, 2 * record_count); return 0;
}
