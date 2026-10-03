#define _POSIX_C_SOURCE 200809L
/* Genuine librdkafka2.15 callback-provider peer. No built-in OIDC claim. */
#include "rdkafka.h"
#include "cJSON.h"
#include <curl/curl.h>
#include <openssl/evp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdatomic.h>
#include <time.h>

#define BODY_MAX 65536U
#define TOKEN_MAX 32768U
struct provider {
    const char *endpoint;
    const char *ca;
    char *secret;
    atomic_uint refreshes;
    atomic_uint failures;
    atomic_uint errors;
};
struct body { unsigned char bytes[BODY_MAX + 1]; size_t length; size_t header_bytes; };
static void fail(const char *label) {
    fprintf(stderr, "{\"status\":\"failed\",\"label\":\"%s\"}\n", label);
    exit(1);
}
static void require(int condition, const char *label) { if (!condition) fail(label); }
static void wipe(void *memory, size_t length) {
    volatile unsigned char *bytes = memory;
    while (length--) *bytes++ = 0;
}
static char *secret_file(const char *path) {
    FILE *file = fopen(path, "rb");
    require(file != NULL, "open secret file");
    char *value = calloc(4098, 1);
    require(value != NULL, "secret reservation");
    size_t length = fread(value, 1, 4097, file);
    require(!ferror(file) && fclose(file) == 0 && length > 0 && length <= 4096,
            "bounded secret file");
    require(memchr(value, 0, length) == NULL, "secret text form");
    return value;
}
static size_t receive(void *data, size_t width, size_t count, void *opaque) {
    struct body *body = opaque;
    if (width && count > SIZE_MAX / width) return 0;
    size_t bytes = width * count;
    if (bytes > BODY_MAX - body->length) return 0;
    memcpy(body->bytes + body->length, data, bytes);
    body->length += bytes;
    body->bytes[body->length] = 0;
    return bytes;
}
static size_t receive_header(void *data, size_t width, size_t count, void *opaque) {
    (void)data;
    struct body *body = opaque;
    if (width && count > SIZE_MAX / width) return 0;
    size_t bytes = width * count;
    if (bytes > 16384 - body->header_bytes) return 0;
    body->header_bytes += bytes;
    return bytes;
}
static int claims(const char *token, char *subject, int64_t *expiration) {
    const char *first = strchr(token, '.');
    if (!first) return 0;
    const char *second = strchr(first + 1, '.');
    if (!second || strchr(second + 1, '.')) return 0;
    size_t length = (size_t)(second - first - 1);
    if (!length || length > 8192) return 0;
    unsigned char encoded[8197], decoded[8197];
    memcpy(encoded, first + 1, length);
    for (size_t i = 0; i < length; i++) {
        if (encoded[i] == '-') encoded[i] = '+';
        else if (encoded[i] == '_') encoded[i] = '/';
    }
    size_t padding = (4 - length % 4) % 4;
    for (size_t i = 0; i < padding; i++) encoded[length + i] = '=';
    encoded[length + padding] = 0;
    int amount = EVP_DecodeBlock(decoded, encoded, (int)(length + padding));
    if (amount < (int)padding) return 0;
    decoded[(size_t)amount - padding] = 0;
    cJSON *value = cJSON_Parse((const char *)decoded);
    const cJSON *sub = cJSON_GetObjectItemCaseSensitive(value, "sub");
    const cJSON *exp = cJSON_GetObjectItemCaseSensitive(value, "exp");
    int valid = cJSON_IsString(sub) && sub->valuestring &&
        strlen(sub->valuestring) > 0 && strlen(sub->valuestring) <= 256 &&
        cJSON_IsNumber(exp) && exp->valuedouble > 0 && exp->valuedouble < 9000000000.0 &&
        (double)(int64_t)exp->valuedouble == exp->valuedouble;
    if (valid) {
        memcpy(subject, sub->valuestring, strlen(sub->valuestring) + 1);
        *expiration = (int64_t)exp->valuedouble * 1000;
    }
    cJSON_Delete(value);
    wipe(encoded, sizeof(encoded)); wipe(decoded, sizeof(decoded));
    return valid;
}
static void refresh(rd_kafka_t *client, const char *config, void *opaque) {
    (void)config;
    struct provider *provider = opaque;
    unsigned ordinal = atomic_fetch_add(&provider->refreshes, 1);
    CURL *http = NULL;
    struct curl_slist *headers = NULL;
    struct body *body = NULL;
    cJSON *response = NULL;
    int success = 0;
    if (ordinal >= 64) goto done;
    http = curl_easy_init(); body = calloc(1, sizeof(*body));
    if (!http || !body) goto done;
    headers = curl_slist_append(headers, "Accept: application/json");
    headers = curl_slist_append(headers, "Content-Type: application/x-www-form-urlencoded");
    if (!headers) goto done;
#define OPTION(name, value) if (curl_easy_setopt(http, name, value) != CURLE_OK) goto done
    OPTION(CURLOPT_URL, provider->endpoint);
    OPTION(CURLOPT_PROTOCOLS_STR, "https");
    OPTION(CURLOPT_REDIR_PROTOCOLS_STR, "https");
    OPTION(CURLOPT_FOLLOWLOCATION, 0L);
    OPTION(CURLOPT_MAXREDIRS, 0L);
    OPTION(CURLOPT_SSL_VERIFYPEER, 1L);
    OPTION(CURLOPT_SSL_VERIFYHOST, 2L);
    OPTION(CURLOPT_CAINFO, provider->ca);
    OPTION(CURLOPT_HTTPAUTH, (long)CURLAUTH_BASIC);
    OPTION(CURLOPT_USERNAME, "partitionline-probe");
    OPTION(CURLOPT_PASSWORD, provider->secret);
    OPTION(CURLOPT_HTTPHEADER, headers);
    OPTION(CURLOPT_HTTP_VERSION, (long)CURL_HTTP_VERSION_1_1);
    OPTION(CURLOPT_POSTFIELDS, "grant_type=client_credentials&scope=kafka");
    OPTION(CURLOPT_TIMEOUT_MS, 2000L);
    OPTION(CURLOPT_CONNECTTIMEOUT_MS, 1000L);
    OPTION(CURLOPT_NOSIGNAL, 1L);
    OPTION(CURLOPT_WRITEFUNCTION, receive);
    OPTION(CURLOPT_WRITEDATA, body);
    OPTION(CURLOPT_HEADERFUNCTION, receive_header);
    OPTION(CURLOPT_HEADERDATA, body);
    if (curl_easy_perform(http) != CURLE_OK) goto done;
    long status = 0; char *media = NULL;
    if (curl_easy_getinfo(http, CURLINFO_RESPONSE_CODE, &status) != CURLE_OK || status != 200 ||
        curl_easy_getinfo(http, CURLINFO_CONTENT_TYPE, &media) != CURLE_OK || !media ||
        strncasecmp(media, "application/json", 16) || (media[16] && media[16] != ';')) goto done;
    response = cJSON_Parse((const char *)body->bytes);
    const cJSON *token = cJSON_GetObjectItemCaseSensitive(response, "access_token");
    const cJSON *type = cJSON_GetObjectItemCaseSensitive(response, "token_type");
    if (!cJSON_IsString(token) || !token->valuestring || !strlen(token->valuestring) ||
        strlen(token->valuestring) > TOKEN_MAX || !cJSON_IsString(type) ||
        !type->valuestring || strcmp(type->valuestring, "Bearer")) goto done;
    char subject[257], error[256]; int64_t expiration = 0;
    if (!claims(token->valuestring, subject, &expiration)) goto done;
    success = rd_kafka_oauthbearer_set_token(client, token->valuestring, expiration,
        subject, NULL, 0, error, sizeof(error)) == RD_KAFKA_RESP_ERR_NO_ERROR;
    wipe(subject, sizeof(subject)); wipe(error, sizeof(error));
done:
    if (!success) {
        atomic_fetch_add(&provider->failures, 1);
        (void)rd_kafka_oauthbearer_set_token_failure(client, "bounded HTTPS provider failure");
    }
    printf("{\"event\":\"token-callback\",\"ordinal\":%u,\"accepted\":%s}\n",
           ordinal, success ? "true" : "false");
    cJSON_Delete(response);
    if (body) { wipe(body, sizeof(*body)); free(body); }
    curl_slist_free_all(headers);
    if (http) curl_easy_cleanup(http);
#undef OPTION
}
static void log_event(const rd_kafka_t *client, int level, const char *facility, const char *text) {
    (void)client; (void)level; (void)facility; (void)text;
}
static void error_event(rd_kafka_t *client, int error, const char *reason, void *opaque) {
    (void)client; (void)error; (void)reason;
    atomic_fetch_add(&((struct provider *)opaque)->errors, 1);
}
static void set(rd_kafka_conf_t *config, const char *name, const char *value) {
    char error[256];
    require(rd_kafka_conf_set(config, name, value, error, sizeof(error)) == RD_KAFKA_CONF_OK,
            "native configuration accepted");
    wipe(error, sizeof(error));
}
int main(int argc, char **argv) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    require(argc == 5, "usage broker caPem tokenEndpoint clientSecretFile");
    require(!strncmp(argv[3], "https://", 8) && strlen(argv[3]) <= 2048,
            "explicit HTTPS token endpoint");
    require(curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK, "curl initialization");
    struct provider provider = {.endpoint = argv[3], .ca = argv[2], .secret = secret_file(argv[4])};
    rd_kafka_conf_t *config = rd_kafka_conf_new();
    set(config, "bootstrap.servers", argv[1]); set(config, "client.id", "oidc-native-peer");
    set(config, "security.protocol", "SASL_SSL"); set(config, "sasl.mechanism", "OAUTHBEARER");
    set(config, "ssl.ca.location", argv[2]); set(config, "ssl.endpoint.identification.algorithm", "https");
    set(config, "enable.ssl.certificate.verification", "true");
    set(config, "enable.idempotence", "false"); set(config, "allow.auto.create.topics", "false");
    set(config, "socket.timeout.ms", "4000"); set(config, "socket.connection.setup.timeout.ms", "4000");
    set(config, "message.timeout.ms", "5000"); set(config, "topic.metadata.refresh.interval.ms", "-1");
    rd_kafka_conf_set_opaque(config, &provider);
    rd_kafka_conf_set_log_cb(config, log_event); rd_kafka_conf_set_error_cb(config, error_event);
    rd_kafka_conf_set_oauthbearer_token_refresh_cb(config, refresh);
    rd_kafka_conf_enable_sasl_queue(config, 1);
    char error[256];
    rd_kafka_t *client = rd_kafka_new(RD_KAFKA_PRODUCER, config, error, sizeof(error));
    require(client != NULL, "native runtime construction"); wipe(error, sizeof(error));
    rd_kafka_error_t *background = rd_kafka_sasl_background_callbacks_enable(client);
    require(background == NULL, "actual SDK background callback queue");
    puts("{\"event\":\"ready\",\"provider\":\"bounded-libcurl-callback\",\"built_in_oidc\":false}");
    char command[64]; unsigned queries = 0;
    while (fgets(command, sizeof(command), stdin)) {
        if (!strcmp(command, "STOP\n")) break;
        require(!strcmp(command, "METADATA pass\n") || !strcmp(command, "METADATA fail\n"),
                "bounded explicit peer command");
        require(++queries <= 32, "finite native query history");
        int expected = !strcmp(command, "METADATA pass\n");
        const struct rd_kafka_metadata *metadata = NULL;
        rd_kafka_resp_err_t code = rd_kafka_metadata(client, 1, NULL, &metadata, 5000);
        int accepted = code == RD_KAFKA_RESP_ERR_NO_ERROR && metadata && metadata->broker_cnt == 1;
        printf("{\"event\":\"metadata\",\"ordinal\":%u,\"accepted\":%s,\"expected\":%s,\"code\":%d}\n",
               queries, accepted ? "true" : "false", expected ? "true" : "false", code);
        if (metadata) rd_kafka_metadata_destroy(metadata);
        require(accepted == expected, "actual authenticated native metadata result");
    }
    rd_kafka_destroy(client);
    require(rd_kafka_wait_destroyed(5000) == 0, "all native SDK threads joined");
    printf("{\"event\":\"joined\",\"queries\":%u,\"callbacks\":%u,\"provider_failures\":%u,\"sdk_errors\":%u}\n",
           queries, atomic_load(&provider.refreshes), atomic_load(&provider.failures), atomic_load(&provider.errors));
    wipe(provider.secret, strlen(provider.secret)); free(provider.secret);
    curl_global_cleanup(); return 0;
}
