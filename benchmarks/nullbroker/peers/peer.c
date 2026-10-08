/* Genuine librdkafka public-API qualification against the explicit seeded fixture. */
#include <rdkafka.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define COUNT 512
static int delivered, failures;
static uint64_t mix(uint64_t x) {
    x += UINT64_C(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    return x ^ (x >> 31);
}
static void be(unsigned char *p, uint64_t x, size_t n) {
    for (size_t i = 0; i < n; i++) p[n - i - 1] = (unsigned char)(x >> (8 * i));
}
static void record(unsigned char key[16], unsigned char value[100], uint64_t offset) {
    uint64_t hash = mix(UINT64_C(0x5eed0001) * UINT64_C(0x9e3779b97f4a7c15) + offset);
    memset(key, 0, 4); be(key + 4, offset, 8); be(key + 12, hash, 4);
    be(value, offset, 8); be(value + 8, hash, 8);
    for (size_t at = 16; at < 100; at += 8) {
        hash = mix(hash);
        unsigned char word[8]; be(word, hash, 8);
        memcpy(value + at, word, 100 - at < 8 ? 100 - at : 8);
    }
}
static void delivery(rd_kafka_t *rk, const rd_kafka_message_t *m, void *opaque) {
    (void)rk; (void)opaque;
    if (m->err || m->partition != 0 || m->offset != delivered) failures++;
    else delivered++;
}
static void set(rd_kafka_conf_t *c, const char *k, const char *v) {
    char error[512];
    if (rd_kafka_conf_set(c, k, v, error, sizeof error) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s: %s\n", k, error); exit(2);
    }
}
static rd_kafka_t *client(const char *address, int consumer) {
    rd_kafka_conf_t *c = rd_kafka_conf_new();
    set(c, "bootstrap.servers", address); set(c, "debug", "protocol");
    set(c, "client.id", "nullbroker-qualification"); set(c, "socket.timeout.ms", "3000");
    set(c, "socket.connection.setup.timeout.ms", "3000");
    if (consumer) {
        set(c, "group.id", "nullbroker-manual-fixture"); set(c, "enable.auto.commit", "false");
        set(c, "enable.auto.offset.store", "false"); set(c, "check.crcs", "true");
        set(c, "fetch.wait.max.ms", "10"); set(c, "fetch.min.bytes", "1");
    } else {
        set(c, "acks", "all"); set(c, "enable.idempotence", "false");
        set(c, "compression.type", "none"); set(c, "linger.ms", "0");
        set(c, "message.timeout.ms", "4000"); rd_kafka_conf_set_dr_msg_cb(c, delivery);
    }
    char error[512]; rd_kafka_t *rk = rd_kafka_new(consumer ? RD_KAFKA_CONSUMER : RD_KAFKA_PRODUCER, c, error, sizeof error);
    if (!rk) { fprintf(stderr, "%s\n", error); exit(2); }
    return rk;
}
int main(int argc, char **argv) {
    if (argc != 2) return 2;
    if (strcmp(rd_kafka_version_str(), "2.15.0")) return 2;
    rd_kafka_t *p = client(argv[1], 0);
    for (int i = 0; i < COUNT; i++) {
        unsigned char key[16], value[100]; record(key, value, (uint64_t)i);
        rd_kafka_resp_err_t e = rd_kafka_producev(p,
            RD_KAFKA_V_TOPIC("nullbroker-peer"), RD_KAFKA_V_PARTITION(0),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), RD_KAFKA_V_VALUE(value, sizeof value),
            RD_KAFKA_V_KEY(key, sizeof key), RD_KAFKA_V_END);
        if (e) { failures++; fprintf(stderr, "produce: %s\n", rd_kafka_err2str(e)); break; }
    }
    if (rd_kafka_flush(p, 5000)) failures++;
    rd_kafka_destroy(p);
    int fetched = 0;
    if (delivered == COUNT && !failures) {
        rd_kafka_t *c = client(argv[1], 1); rd_kafka_poll_set_consumer(c);
        rd_kafka_topic_partition_list_t *parts = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(parts, "nullbroker-peer", 0)->offset = 0;
        if (rd_kafka_assign(c, parts)) failures++;
        rd_kafka_topic_partition_list_destroy(parts);
        time_t deadline = time(NULL) + 6;
        while (fetched < COUNT && !failures && time(NULL) < deadline) {
            rd_kafka_message_t *m = rd_kafka_consumer_poll(c, 100);
            if (!m) continue;
            unsigned char key[16], value[100]; record(key, value, (uint64_t)fetched);
            rd_kafka_timestamp_type_t timestamp_type;
            int64_t timestamp = rd_kafka_message_timestamp(m, &timestamp_type);
            rd_kafka_headers_t *headers = NULL;
            rd_kafka_resp_err_t header_error = rd_kafka_message_headers(m, &headers);
            if (m->err || timestamp != INT64_C(1700000000000) + (fetched / 128) * 128 || timestamp_type != RD_KAFKA_TIMESTAMP_CREATE_TIME ||
                (header_error != RD_KAFKA_RESP_ERR__NOENT && header_error != RD_KAFKA_RESP_ERR_NO_ERROR) ||
                (headers && rd_kafka_header_cnt(headers) != 0) || m->partition != 0 || m->offset != fetched ||
                m->key_len != sizeof key || m->len != sizeof value ||
                memcmp(m->key, key, sizeof key) || memcmp(m->payload, value, sizeof value)) {
                fprintf(stderr, "fetch validation offset=%lld error=%s\n", (long long)m->offset, rd_kafka_err2str(m->err)); failures++;
            } else fetched++;
            rd_kafka_message_destroy(m);
        }
        rd_kafka_assign(c, NULL); rd_kafka_consumer_close(c); rd_kafka_destroy(c);
    }
    if (fetched != COUNT || delivered != COUNT) failures++;
    if (rd_kafka_wait_destroyed(3000)) failures++;
    printf("{\"peer\":\"librdkafka\",\"version\":\"%s\",\"acknowledged\":%d,\"validated_fetch\":%d,\"validation_failures\":%d,\"fetch_source\":\"seeded-independent-of-produce\"}\n", rd_kafka_version_str(), delivered, fetched, failures);
    return failures ? 1 : 0;
}
