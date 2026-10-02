#ifndef PARTITIONLINE_LIBRDKAFKA_0125_SHIM_H
#define PARTITIONLINE_LIBRDKAFKA_0125_SHIM_H
/* Harness-only API adaptation. Original selected upstream source is unchanged. */
#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include "rdkafka.h"
#include "rdkafka_mock.h"

typedef struct { double start; const char *name; } test_timing_t;
void fixture_timing_start(test_timing_t *,const char *);
void fixture_timing_assert(test_timing_t *,double,double);
void fixture_assert(int,const char *,int);
#define TIMING_START(timer,name) fixture_timing_start((timer),(name))
#define TIMING_ASSERT(timer,lower,upper) fixture_timing_assert((timer),(lower),(upper))
#define TEST_CALL_ERR__(call) fixture_assert((call)==RD_KAFKA_RESP_ERR_NO_ERROR,#call,__LINE__)
#define TEST_SKIP_MOCK_CLUSTER(value) ((void)(value))
const char *test_mk_topic_name(const char *,int);
void test_conf_init(rd_kafka_conf_t **,void *,int);
void test_conf_set(rd_kafka_conf_t *,const char *,const char *);
void test_dr_msg_cb(rd_kafka_t *,const rd_kafka_message_t *,void *);
rd_kafka_t *test_create_handle(rd_kafka_type_t,rd_kafka_conf_t *);
void test_create_topic_wait_exists(rd_kafka_t *,const char *,int,int,int);
void test_produce_msgs2_nowait(rd_kafka_t *,const char *,uint64_t,int,int,int,const void *,size_t,int *);
void test_consume_msgs_easy(const char *,const char *,int,int,int,void *);
rd_kafka_resp_err_t fixture_flush(rd_kafka_t *,int);
#define rd_kafka_flush fixture_flush
/* The brokerless sibling is not selected; discarded by --gc-sections. */
rd_kafka_mock_cluster_t *test_mock_cluster_new(int,const char **);
void test_mock_cluster_destroy(rd_kafka_mock_cluster_t *);
char *rd_strndup(const char *,size_t);
#endif
