/* Development-only observation of actual native SSL_write request shapes.
 * It forwards bytes unchanged. No username, auth token, salt, key or proof is
 * logged or persisted; metadata below is numeric Kafka framing only. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <openssl/ssl.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static pthread_once_t once = PTHREAD_ONCE_INIT;
static int (*forward)(SSL *, const void *, int);
static int (*forward_read)(SSL *, void *, int);
static pthread_mutex_t state_lock = PTHREAD_MUTEX_INITIALIZER;
static struct {
    SSL *socket;
    int32_t correlation;
    int wanted;
    size_t position;
    unsigned char frame[18];
} states[16];
static void initialize(void) {
    *(void **)(&forward) = dlsym(RTLD_NEXT, "SSL_write");
    *(void **)(&forward_read) = dlsym(RTLD_NEXT, "SSL_read");
    if (forward == NULL || forward_read == NULL) abort();
}
static uint32_t number(const unsigned char *p, size_t n) {
    uint32_t result = 0;
    for (size_t i = 0; i < n; i++) result = (result << 8) | p[i];
    return result;
}
static int variable(const unsigned char *p, size_t len, size_t *pos, uint32_t *out) {
    *out = 0;
    for (unsigned shift = 0; shift <= 28; shift += 7) {
        if (*pos == len) return 0;
        unsigned byte = p[(*pos)++];
        if (shift == 28 && byte > 15) return 0;
        *out |= (byte & 127) << shift;
        if (!(byte & 128)) return 1;
    }
    return 0;
}
static void observe(SSL *socket, const unsigned char *p, size_t len) {
    if (len < 14 || number(p, 4) + 4 != len) return;
    unsigned key = number(p + 4, 2), version = number(p + 6, 2);
    int32_t correlation = (int32_t)number(p + 8, 4);
    if (key == 50) {
        pthread_mutex_lock(&state_lock);
        for (size_t i = 0; i < 16; i++) {
            if (states[i].socket == socket || states[i].socket == NULL) {
                states[i].socket = socket; states[i].correlation = correlation;
                states[i].wanted = 1; states[i].position = 0; break;
            }
        }
        pthread_mutex_unlock(&state_lock);
    }
    flockfile(stderr);
    fprintf(stderr, "{\"trace\":\"native-SSL_write\",\"api\":%u,\"version\":%u,\"correlation\":%d,\"frame_bytes\":%zu}\n", key, version, correlation, len);
    if (key == 51) {
        unsigned char digest[SHA256_DIGEST_LENGTH];
        SHA256(p, len, digest);
        fprintf(stderr, "{\"trace\":\"native-Alter-public-fixture-frame-hash\",\"sha256\":\"");
        for (size_t i = 0; i < sizeof(digest); i++) fprintf(stderr, "%02x", digest[i]);
        fprintf(stderr, "\"}\n");
        size_t pos = 14;
        int16_t client = (int16_t)number(p + 12, 2);
        uint32_t tags = 999, deletions = 999, upserts = 999, name = 999;
        uint32_t salt = 999, salted = 999, structure_tags = 999, body_tags = 999;
        unsigned algorithm = 999, iterations = 999;
        int valid = client >= -1 && (client < 0 || (size_t)client <= len - pos);
        if (valid && client > 0) pos += (size_t)client;
        valid = valid && variable(p, len, &pos, &tags) && tags == 0;
        valid = valid && variable(p, len, &pos, &deletions) && deletions == 1;
        valid = valid && variable(p, len, &pos, &upserts) && upserts == 2;
        valid = valid && variable(p, len, &pos, &name) && name > 0 && name - 1 <= len - pos;
        if (valid) pos += name - 1;
        valid = valid && len - pos >= 5;
        if (valid) { algorithm = p[pos++]; iterations = number(p + pos, 4); pos += 4; }
        valid = valid && variable(p, len, &pos, &salt) && salt > 0 && salt - 1 <= len - pos;
        if (valid) pos += salt - 1;
        valid = valid && variable(p, len, &pos, &salted) && salted > 0 && salted - 1 <= len - pos;
        if (valid) pos += salted - 1;
        valid = valid && variable(p, len, &pos, &structure_tags) && structure_tags == 0;
        valid = valid && variable(p, len, &pos, &body_tags) && body_tags == 0 && pos == len;
        int zero_tail = 1;
        for (size_t i = pos; i < len; i++) if (p[i] != 0) zero_tail = 0;
        fprintf(stderr, "{\"trace\":\"native-Alter-numeric-shape\",\"header_tags\":%u,\"deletions_compact_count\":%u,\"upserts_compact_count\":%u,\"name_compact_length\":%u,\"algorithm\":%u,\"iterations\":%u,\"salt_compact_length\":%u,\"salted_compact_length\":%u,\"struct_tags\":%u,\"body_tags\":%u,\"trailing_bytes\":%zu,\"trailing_all_zero\":%s,\"canonical_shape\":%s}\n", tags, deletions, upserts, name, algorithm, iterations, salt, salted, structure_tags, body_tags, len - pos, zero_tail ? "true" : "false", valid ? "true" : "false");
    }
    funlockfile(stderr);
}
int SSL_write(SSL *socket, const void *data, int length) {
    pthread_once(&once, initialize);
    if (length > 0) observe(socket, data, (size_t)length);
    return forward(socket, data, length);
}
int SSL_read(SSL *socket, void *data, int length) {
    pthread_once(&once, initialize);
    int result = forward_read(socket, data, length), captured = 0;
    unsigned char frame[18];
    if (result > 0) {
        pthread_mutex_lock(&state_lock);
        for (size_t i = 0; i < 16; i++) {
            if (states[i].socket != socket || !states[i].wanted) continue;
            if ((size_t)result > 18 - states[i].position) { states[i].wanted = 0; break; }
            memcpy(states[i].frame + states[i].position, data, (size_t)result);
            states[i].position += (size_t)result;
            if (states[i].position >= 4 && number(states[i].frame, 4) != 14) states[i].wanted = 0;
            if (states[i].position == 18) {
                unsigned char *p = states[i].frame;
                // Only this exact public error response is retained. No
                // authentication/proof frame is copied to an output or log.
                if (number(p + 4, 4) == (uint32_t)states[i].correlation && p[8] == 0 &&
                    number(p + 9, 4) == 0 && number(p + 13, 2) == 31 &&
                    p[15] == 0 && p[16] == 1 && p[17] == 0) {
                    memcpy(frame, p, 18); captured = 1;
                }
                states[i].wanted = 0;
            }
            if (!states[i].wanted) memset(states[i].frame, 0, 18);
            break;
        }
        pthread_mutex_unlock(&state_lock);
    }
    if (captured) {
        flockfile(stderr);
        fprintf(stderr, "{\"trace\":\"native-Describe-public-error31-response\",\"frame_hex\":\"");
        for (size_t i = 0; i < sizeof(frame); i++) fprintf(stderr, "%02x", frame[i]);
        fprintf(stderr, "\"}\n");
        funlockfile(stderr);
        memset(frame, 0, sizeof(frame));
    }
    return result;
}
