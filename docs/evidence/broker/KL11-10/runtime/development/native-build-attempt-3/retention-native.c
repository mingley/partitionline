/* Pinned librdkafka public DeleteRecords and manual retained Java-batch reads. */
#include <inttypes.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rdkafka.h>

static unsigned assertions;
static char history[65536];
static size_t used;
static void check(int condition, const char *label) {
    assertions++;
    if (!condition) { fprintf(stderr, "assertion failed: %s\n", label); exit(1); }
}
static void append(const char *format, ...) {
    va_list args; va_start(args, format);
    int n = vsnprintf(history + used, sizeof(history) - used, format, args); va_end(args);
    check(n >= 0 && (size_t)n < sizeof(history) - used, "bounded retained history");
    used += (size_t)n;
}
static void config(rd_kafka_conf_t *conf, const char *name, const char *value) {
    char error[512]; check(rd_kafka_conf_set(conf, name, value, error, sizeof(error)) == RD_KAFKA_CONF_OK, name);
}
static int equal(const void *data, size_t length, const char *text) { return data && length == strlen(text) && memcmp(data, text, length) == 0; }
static void hex(FILE *file, const void *data, size_t length) {
    if (!data) { fputs("null", file); return; }
    fputc('"', file);
    const unsigned char *bytes = data;
    for (size_t i = 0; i < length; i++) fprintf(file, "%02x", bytes[i]);
    fputc('"', file);
}
static void deletion(rd_kafka_t *client, const char *topic, int64_t offset, int64_t low, rd_kafka_resp_err_t code) {
    rd_kafka_topic_partition_list_t *input = rd_kafka_topic_partition_list_new(1);
    check(input != NULL, "bounded deletion input"); rd_kafka_topic_partition_list_add(input, topic, 0)->offset = offset;
    rd_kafka_DeleteRecords_t *request = rd_kafka_DeleteRecords_new(input); check(request != NULL, "public DeleteRecords request");
    rd_kafka_AdminOptions_t *options = rd_kafka_AdminOptions_new(client, RD_KAFKA_ADMIN_OP_DELETERECORDS);
    check(options != NULL, "admin options"); char error[512];
    check(rd_kafka_AdminOptions_set_operation_timeout(options, 5000, error, sizeof(error)) == RD_KAFKA_RESP_ERR_NO_ERROR, "operation timeout");
    check(rd_kafka_AdminOptions_set_request_timeout(options, 10000, error, sizeof(error)) == RD_KAFKA_RESP_ERR_NO_ERROR, "request timeout");
    rd_kafka_queue_t *queue = rd_kafka_queue_new(client); check(queue != NULL, "admin queue");
    rd_kafka_DeleteRecords(client, &request, 1, options, queue);
    rd_kafka_event_t *event = rd_kafka_queue_poll(queue, 15000); check(event != NULL && rd_kafka_event_type(event) == RD_KAFKA_EVENT_DELETERECORDS_RESULT, "actual DeleteRecords event");
    check(rd_kafka_event_error(event) == RD_KAFKA_RESP_ERR_NO_ERROR, "actual admin envelope");
    const rd_kafka_topic_partition_list_t *results = rd_kafka_DeleteRecords_result_offsets(rd_kafka_event_DeleteRecords_result(event));
    check(results != NULL && results->cnt == 1 && strcmp(results->elems[0].topic, topic) == 0 && results->elems[0].partition == 0, "exact delete target");
    check(results->elems[0].offset == low && results->elems[0].err == code, "actual DeleteRecords low watermark/error");
    append("%s{\"label\":\"actual-native-delete\",\"offset\":%" PRId64 ",\"low_watermark\":%" PRId64 ",\"error_code\":%d}", used ? "," : "", offset, results->elems[0].offset, (int)results->elems[0].err);
    rd_kafka_event_destroy(event); rd_kafka_queue_destroy(queue); rd_kafka_AdminOptions_destroy(options); rd_kafka_DeleteRecords_destroy(request); rd_kafka_topic_partition_list_destroy(input);
}
int main(int argc, char **argv) {
    if (argc != 6) { fprintf(stderr, "bootstrap topic seed|restart end output-json\n"); return 2; }
    const char *topic = argv[2]; const char *phase = argv[3]; long end = strtol(argv[4], NULL, 10);
    check(strlen(argv[1]) <= 128 && strlen(topic) > 0 && strlen(topic) <= 64 && strspn(topic, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-") == strlen(topic), "bounded arguments");
    check((strcmp(phase, "seed") == 0 || strcmp(phase, "restart") == 0) && (end == 7 || end == 8), "bounded phase/end");
    rd_kafka_conf_t *conf = rd_kafka_conf_new(); check(conf != NULL, "configuration");
    config(conf, "bootstrap.servers", argv[1]); config(conf, "client.id", "retention-native"); config(conf, "group.id", "retention-native-manual");
    config(conf, "enable.auto.commit", "false"); config(conf, "enable.auto.offset.store", "false"); config(conf, "allow.auto.create.topics", "false");
    config(conf, "auto.offset.reset", "error"); config(conf, "fetch.wait.max.ms", "20"); config(conf, "fetch.min.bytes", "1");
    config(conf, "message.max.bytes", "65536"); config(conf, "fetch.message.max.bytes", "4096"); config(conf, "fetch.max.bytes", "65536"); config(conf, "socket.timeout.ms", "5000");
    char error[512]; rd_kafka_t *client = rd_kafka_new(RD_KAFKA_CONSUMER, conf, error, sizeof(error));
    if (!client) fprintf(stderr, "native construction error: %s\n", error);
    check(client != NULL, "actual native client");
    const struct rd_kafka_metadata *metadata = NULL;
    check(rd_kafka_metadata(client, 1, NULL, &metadata, 5000) == RD_KAFKA_RESP_ERR_NO_ERROR && metadata != NULL && metadata->broker_cnt == 1, "actual unchanged all-topics metadata query");
    rd_kafka_metadata_destroy(metadata);
    deletion(client, topic, (int64_t)end + 1, -1, RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE);
    deletion(client, topic, 3, 3, RD_KAFKA_RESP_ERR_NO_ERROR); deletion(client, topic, 1, 3, RD_KAFKA_RESP_ERR_NO_ERROR);
    check(rd_kafka_poll_set_consumer(client) == RD_KAFKA_RESP_ERR_NO_ERROR, "consumer poll routing");
    rd_kafka_topic_partition_list_t *assignment = rd_kafka_topic_partition_list_new(1); check(assignment != NULL, "assignment");
    rd_kafka_topic_partition_list_add(assignment, topic, 0)->offset = 3;
    check(rd_kafka_assign(client, assignment) == RD_KAFKA_RESP_ERR_NO_ERROR, "manual assignment without group join");
    rd_kafka_topic_partition_list_destroy(assignment);
    FILE *file = fopen(argv[5], "w"); check(file != NULL, "receipt output");
    fprintf(file, "{\"phase\":\"%s\",\"topic\":\"%s\",\"history\":[%s", phase, topic, history);
    long next = 3; int polls = 0; const int64_t times[] = {1000,1007,1003,1007,1010,1011,1012,1013};
    while (next < end && polls++ < 100) {
        rd_kafka_message_t *record = rd_kafka_consumer_poll(client, 200);
        if (!record) continue;
        check(record->err == RD_KAFKA_RESP_ERR_NO_ERROR && record->partition == 0 && record->offset == next && strcmp(rd_kafka_topic_name(record->rkt), topic) == 0, "retained offset/order/no deleted prefix");
        rd_kafka_timestamp_type_t timestamp_type; int64_t timestamp = rd_kafka_message_timestamp(record, &timestamp_type);
        check(timestamp_type == RD_KAFKA_TIMESTAMP_CREATE_TIME && timestamp == times[next], "retained CreateTime");
        char expected[256]; snprintf(expected, sizeof(expected), "key:%s:%ld", topic, next);
        check(next % 3 == 0 ? record->key == NULL : equal(record->key, record->key_len, expected), "exact key/null bytes");
        snprintf(expected, sizeof(expected), "value:%s:%ld", topic, next);
        check(next % 3 == 0 ? record->payload == NULL : next % 3 == 1 ? record->payload != NULL && record->len == 0 : equal(record->payload, record->len, expected), "exact value/null/empty bytes");
        rd_kafka_headers_t *headers = NULL; check(rd_kafka_message_headers(record, &headers) == RD_KAFKA_RESP_ERR_NO_ERROR && rd_kafka_header_cnt(headers) == 3, "ordered duplicate headers");
        const char *name; const void *value; size_t length;
        snprintf(expected, sizeof(expected), "%s:%ld", topic, next);
        for (size_t i = 0; i < 3; i++) {
            check(rd_kafka_header_get_all(headers, i, &name, &value, &length) == RD_KAFKA_RESP_ERR_NO_ERROR, "retained header access");
            check(strcmp(name, i == 0 ? "receipt" : "dup") == 0 && (i == 0 ? equal(value,length,expected) : i == 1 ? equal(value,length,"a") : value == NULL), "exact ordered header bytes");
        }
        fprintf(file, ",{\"label\":\"retained-native-consumer\",\"record\":{\"topic\":\"%s\",\"partition\":0,\"offset\":%" PRId64 ",\"timestamp\":%" PRId64 ",\"key_hex\":", topic, record->offset, timestamp);
        hex(file, record->key, record->key_len); fputs(",\"value_hex\":", file); hex(file, record->payload, record->len); fputs(",\"headers\":[", file);
        for (size_t i = 0; i < 3; i++) { check(rd_kafka_header_get_all(headers,i,&name,&value,&length) == RD_KAFKA_RESP_ERR_NO_ERROR,"header output"); fprintf(file,"%s{\"key\":\"%s\",\"value_hex\":",i?",":"",name); hex(file,value,length); fputc('}',file); }
        fputs("]}}",file); rd_kafka_message_destroy(record); next++;
    }
    check(next == end, "all retained records consumed by genuine native Consumer");
    int64_t low, high; check(rd_kafka_query_watermark_offsets(client,topic,0,&low,&high,5000) == RD_KAFKA_RESP_ERR_NO_ERROR && low == 3 && high == end,"actual native logical start/end");
    check(rd_kafka_consumer_close(client) == RD_KAFKA_RESP_ERR_NO_ERROR,"native close"); rd_kafka_destroy(client);
    fprintf(file,"],\"records\":%ld,\"assertions\":%u,\"passed\":true}\n",end-3,assertions); check(fclose(file) == 0,"receipt close");
    printf("{\"records\":%ld,\"assertions\":%u,\"passed\":true}\n",end-3,assertions); return 0;
}
