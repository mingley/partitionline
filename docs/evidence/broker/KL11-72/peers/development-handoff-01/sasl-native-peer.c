/* Actual pinned librdkafka/OpenSSL runtime, external test peer only.
 * Synthetic fixture passwords are never printed; native log text is discarded.
 */
#include "rdkafka.h"
#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

static int assertions;
static atomic_int auth_errors;
static atomic_int native_logs;
static const unsigned char salt[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
static void require(int condition, const char *label) {
    assertions++;
    if (!condition) { fprintf(stderr, "assertion failed: %s\n", label); exit(1); }
}
static void log_cb(const rd_kafka_t *client, int level, const char *facility, const char *text) {
    (void)client; (void)level; (void)facility; (void)text;
    atomic_fetch_add_explicit(&native_logs, 1, memory_order_relaxed);
}
static void error_cb(rd_kafka_t *client, int error, const char *reason, void *opaque) {
    (void)client; (void)reason; (void)opaque;
    if (error == RD_KAFKA_RESP_ERR__AUTHENTICATION)
        atomic_fetch_add_explicit(&auth_errors, 1, memory_order_relaxed);
}
static void set(rd_kafka_conf_t *config, const char *name, const char *value) {
    char error[512];
    require(rd_kafka_conf_set(config, name, value, error, sizeof(error)) == RD_KAFKA_CONF_OK, name);
}
static rd_kafka_t *client(const char *broker, const char *ca, const char *mechanism,
                          const char *user, const char *password, int tls) {
    rd_kafka_conf_t *config = rd_kafka_conf_new();
    set(config, "bootstrap.servers", broker); set(config, "client.id", "sasl-native-peer");
    set(config, "security.protocol", tls ? "SASL_SSL" : "SASL_PLAINTEXT");
    set(config, "sasl.mechanism", mechanism); set(config, "sasl.username", user); set(config, "sasl.password", password);
    set(config, "socket.timeout.ms", "5000"); set(config, "socket.connection.setup.timeout.ms", "5000");
    set(config, "reconnect.backoff.ms", "1000"); set(config, "reconnect.backoff.max.ms", "1000");
    set(config, "allow.auto.create.topics", "false");
    if (tls) {
        set(config, "ssl.ca.location", ca); set(config, "enable.ssl.certificate.verification", "true");
        set(config, "ssl.endpoint.identification.algorithm", "https");
    }
    rd_kafka_conf_set_log_cb(config, log_cb); rd_kafka_conf_set_error_cb(config, error_cb);
    char error[512];
    rd_kafka_t *result = rd_kafka_new(RD_KAFKA_PRODUCER, config, error, sizeof(error));
    require(result != NULL, "native runtime allocation");
    return result;
}
static void connection(const char *broker, const char *ca, const char *mechanism,
                       const char *user, const char *password, int tls, int success) {
    atomic_store_explicit(&auth_errors, 0, memory_order_relaxed);
    rd_kafka_t *runtime = client(broker, ca, mechanism, user, password, tls);
    const struct rd_kafka_metadata *metadata = NULL;
    rd_kafka_resp_err_t error = rd_kafka_metadata(runtime, 1, NULL, &metadata, 6000);
    rd_kafka_poll(runtime, 100);
    if (success) {
        require(error == RD_KAFKA_RESP_ERR_NO_ERROR && metadata != NULL && metadata->broker_cnt == 1,
                "authenticated native metadata discovery");
        require(metadata->brokers[0].id == 0, "real node0 metadata");
    } else {
        require(error != RD_KAFKA_RESP_ERR_NO_ERROR && atomic_load_explicit(&auth_errors, memory_order_relaxed) > 0,
                "actual native authentication failure callback");
    }
    printf("{\"operation\":\"native-session\",\"mechanism\":\"%s\",\"tls\":%s,\"expected_success\":%s,\"metadata_error\":%d,\"authentication_callbacks\":%d}\n",
           mechanism, tls ? "true" : "false", success ? "true" : "false", (int)error,
           atomic_load_explicit(&auth_errors, memory_order_relaxed));
    if (metadata != NULL) rd_kafka_metadata_destroy(metadata);
    rd_kafka_destroy(runtime);
}
static rd_kafka_AdminOptions_t *options(rd_kafka_t *runtime, rd_kafka_admin_op_t operation) {
    rd_kafka_AdminOptions_t *result = rd_kafka_AdminOptions_new(runtime, operation);
    require(result != NULL, "admin options allocation");
    char error[512];
    require(rd_kafka_AdminOptions_set_request_timeout(result, 5000, error, sizeof(error)) == RD_KAFKA_RESP_ERR_NO_ERROR,
            "bounded admin request");
    return result;
}
static void describe(rd_kafka_t *runtime, rd_kafka_queue_t *queue, const char *user, int expected, size_t algorithms) {
    rd_kafka_AdminOptions_t *opts = options(runtime, RD_KAFKA_ADMIN_OP_DESCRIBEUSERSCRAMCREDENTIALS);
    rd_kafka_DescribeUserScramCredentials(runtime, &user, 1, opts, queue);
    rd_kafka_event_t *event = rd_kafka_queue_poll(queue, 8000);
    require(event != NULL && rd_kafka_event_type(event) == RD_KAFKA_EVENT_DESCRIBEUSERSCRAMCREDENTIALS_RESULT,
            "native describe result event");
    int error = (int)rd_kafka_event_error(event);
    size_t count = 0, actual_algorithms = 0;
    if (error == 0) {
        const rd_kafka_UserScramCredentialsDescription_t **rows = rd_kafka_DescribeUserScramCredentials_result_descriptions(
                rd_kafka_event_DescribeUserScramCredentials_result(event), &count);
        require(count == 1 && strcmp(rd_kafka_UserScramCredentialsDescription_user(rows[0]), user) == 0,
                "native describe exact user");
        const rd_kafka_error_t *failure = rd_kafka_UserScramCredentialsDescription_error(rows[0]);
        if (failure != NULL) error = (int)rd_kafka_error_code(failure);
        actual_algorithms = rd_kafka_UserScramCredentialsDescription_scramcredentialinfo_count(rows[0]);
        require(actual_algorithms == algorithms, "native metadata-only credential count");
        for (size_t i = 0; i < actual_algorithms; i++) {
            const rd_kafka_ScramCredentialInfo_t *info = rd_kafka_UserScramCredentialsDescription_scramcredentialinfo(rows[0], i);
            require(rd_kafka_ScramCredentialInfo_iterations(info) == 4096, "native metadata iterations");
        }
    }
    printf("{\"operation\":\"native-describe\",\"user\":\"%s\",\"error\":%d,\"algorithms\":%zu}\n", user, error, actual_algorithms);
    require(error == expected, "native describe expected status");
    rd_kafka_event_destroy(event); rd_kafka_AdminOptions_destroy(opts);
}
static void alter(rd_kafka_t *runtime, rd_kafka_queue_t *queue, const char *user, const char *password, int deletion, int expected) {
    rd_kafka_UserScramCredentialAlteration_t *change = deletion
            ? rd_kafka_UserScramCredentialDeletion_new(user, RD_KAFKA_SCRAM_MECHANISM_SHA_256)
            : rd_kafka_UserScramCredentialUpsertion_new(user, RD_KAFKA_SCRAM_MECHANISM_SHA_256, 4096,
                    (const unsigned char *)password, strlen(password), salt, sizeof(salt));
    require(change != NULL, "actual OpenSSL SCRAM alteration allocation");
    rd_kafka_AdminOptions_t *opts = options(runtime, RD_KAFKA_ADMIN_OP_ALTERUSERSCRAMCREDENTIALS);
    rd_kafka_AlterUserScramCredentials(runtime, &change, 1, opts, queue);
    rd_kafka_event_t *event = rd_kafka_queue_poll(queue, 8000);
    require(event != NULL && rd_kafka_event_type(event) == RD_KAFKA_EVENT_ALTERUSERSCRAMCREDENTIALS_RESULT,
            "native alter result event");
    int envelope_error = (int)rd_kafka_event_error(event);
    printf("{\"operation\":\"native-alter-envelope\",\"error\":%d}\n", envelope_error);
    require(envelope_error == RD_KAFKA_RESP_ERR_NO_ERROR, "native alter envelope success");
    size_t count = 0;
    const rd_kafka_AlterUserScramCredentials_result_response_t **rows = rd_kafka_AlterUserScramCredentials_result_responses(
            rd_kafka_event_AlterUserScramCredentials_result(event), &count);
    require(count == 1 && strcmp(rd_kafka_AlterUserScramCredentials_result_response_user(rows[0]), user) == 0,
            "native alter exact user");
    const rd_kafka_error_t *failure = rd_kafka_AlterUserScramCredentials_result_response_error(rows[0]);
    int error = failure == NULL ? 0 : (int)rd_kafka_error_code(failure);
    printf("{\"operation\":\"native-%s\",\"user\":\"%s\",\"error\":%d}\n", deletion ? "delete" : "upsert", user, error);
    require(error == expected, "native alter expected status");
    rd_kafka_event_destroy(event); rd_kafka_AdminOptions_destroy(opts); rd_kafka_UserScramCredentialAlteration_destroy(change);
}
int main(int argc, char **argv) {
    require(setvbuf(stdout, NULL, _IOLBF, 0) == 0, "line-buffered public receipts");
    require(argc == 5, "usage tlsBroker plainBroker caPem sessions|admin|restart");
    require(strcmp(rd_kafka_version_str(), "2.15.0") == 0, "pinned native runtime version");
    printf("{\"peer\":\"librdkafka\",\"version\":\"%s\",\"phase\":\"%s\"}\n", rd_kafka_version_str(), argv[4]);
    if (strcmp(argv[4], "sessions") == 0) {
        const char *mechanisms[] = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"};
        for (size_t i = 0; i < 3; i++) {
            connection(argv[1], argv[3], mechanisms[i], "user", "pencil", 1, 1);
            connection(argv[1], argv[3], mechanisms[i], "user", "wrong-public-fixture", 1, 0);
            if (i != 0) {
                connection(argv[2], argv[3], mechanisms[i], "user", "pencil", 0, 1);
                connection(argv[2], argv[3], mechanisms[i], "user", "wrong-public-fixture", 0, 0);
            }
        }
        connection(argv[2], argv[3], "PLAIN", "user", "pencil", 0, 0);
    } else if (strcmp(argv[4], "describe-user-denied") == 0 || strcmp(argv[4], "describe-admin-denied") == 0) {
        const char *user = strcmp(argv[4], "describe-user-denied") == 0 ? "user" : "admin";
        rd_kafka_t *runtime = client(argv[1], argv[3], "SCRAM-SHA-256", user, "pencil", 1);
        rd_kafka_queue_t *queue = rd_kafka_queue_new(runtime);
        require(queue != NULL, "native isolated describe queue");
        describe(runtime, queue, "user", 31, 0);
        rd_kafka_queue_destroy(queue); rd_kafka_destroy(runtime);
    } else {
        int restart = strcmp(argv[4], "restart") == 0;
        int denied = strcmp(argv[4], "admin-denied") == 0;
        require(restart || denied || strcmp(argv[4], "admin") == 0, "bounded phase name");
        rd_kafka_t *runtime = client(argv[1], argv[3], "SCRAM-SHA-256", "admin", "pencil", 1);
        rd_kafka_queue_t *queue = rd_kafka_queue_new(runtime);
        require(queue != NULL, "native admin queue");
        if (denied) {
            alter(runtime, queue, "default-forbidden", "pencil", 0, 31);
            connection(argv[1], argv[3], "SCRAM-SHA-256", "native-created", "native-rotated-public-fixture", 1, 0);
        } else if (!restart) {
            describe(runtime, queue, "native-created", 91, 0);
            alter(runtime, queue, "native-created", "pencil", 0, 0); describe(runtime, queue, "native-created", 0, 1);
            alter(runtime, queue, "native-created", "native-rotated-public-fixture", 0, 0);
            connection(argv[1], argv[3], "SCRAM-SHA-256", "native-created", "pencil", 1, 0);
            connection(argv[1], argv[3], "SCRAM-SHA-256", "native-created", "native-rotated-public-fixture", 1, 1);
        } else {
            connection(argv[1], argv[3], "SCRAM-SHA-256", "native-created", "native-rotated-public-fixture", 1, 1);
            describe(runtime, queue, "native-created", 0, 1); alter(runtime, queue, "native-created", NULL, 1, 0);
            describe(runtime, queue, "native-created", 91, 0); alter(runtime, queue, "native-created", NULL, 1, 91);
        }
        rd_kafka_queue_destroy(queue); rd_kafka_destroy(runtime);
        if (!restart && !denied) {
            runtime = client(argv[1], argv[3], "SCRAM-SHA-256", "user", "pencil", 1);
            queue = rd_kafka_queue_new(runtime);
            alter(runtime, queue, "native-forbidden", "pencil", 0, 31);
            rd_kafka_queue_destroy(queue); rd_kafka_destroy(runtime);
        }
    }
    require(rd_kafka_wait_destroyed(5000) == 0, "native runtime joined cleanup");
    printf("{\"status\":\"pass\",\"assertions\":%d,\"suppressed_native_log_messages\":%d}\n",
           assertions, atomic_load_explicit(&native_logs, memory_order_relaxed));
    return 0;
}
