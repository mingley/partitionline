/* SPDX-License-Identifier: Apache-2.0 */
/* Independent native Kafka decoder; never links partitionline. */
#include "rdkafka.h"
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
static uint64_t mix(uint64_t x) {x+=UINT64_C(0x9e3779b97f4a7c15);x=(x^(x>>30))*UINT64_C(0xbf58476d1ce4e5b9);x=(x^(x>>27))*UINT64_C(0x94d049bb133111eb);return x^(x>>31);}
static void be64(unsigned char *p,uint64_t n) {for(int j=7;j>=0;j--){p[j]=(unsigned char)n;n>>=8;}}
static int set(rd_kafka_conf_t *c,const char *k,const char *v) {char error[256];if(rd_kafka_conf_set(c,k,v,error,sizeof error)!=RD_KAFKA_CONF_OK){fprintf(stderr,"%s\n",error);return 0;}return 1;}
int main(int argc,char **argv) {
 if(argc!=3 || strcmp(rd_kafka_version_str(),"2.15.0")) return 2;
 rd_kafka_conf_t *conf=rd_kafka_conf_new();char error[256];
 if(!set(conf,"bootstrap.servers",argv[1])||!set(conf,"group.id","zstd-independent-c-decoder")||!set(conf,"enable.auto.commit","false")||!set(conf,"enable.auto.offset.store","false")||!set(conf,"socket.timeout.ms","10000")) return 3;
 rd_kafka_t *rk=rd_kafka_new(RD_KAFKA_CONSUMER,conf,error,sizeof error);if(!rk){fprintf(stderr,"%s\n",error);return 4;}
 rd_kafka_poll_set_consumer(rk);rd_kafka_topic_partition_list_t *partitions=rd_kafka_topic_partition_list_new(1);
 rd_kafka_topic_partition_list_add(partitions,argv[2],0)->offset=RD_KAFKA_OFFSET_BEGINNING;
 int failed=rd_kafka_assign(rk,partitions)!=RD_KAFKA_RESP_ERR_NO_ERROR;rd_kafka_topic_partition_list_destroy(partitions);
 int count=0;unsigned char seen[128]={0};time_t deadline=time(NULL)+30;
 while(!failed && count<128 && time(NULL)<deadline) {
  rd_kafka_message_t *m=rd_kafka_consumer_poll(rk,200);if(!m)continue;
  if(m->err){fprintf(stderr,"%s\n",rd_kafka_message_errstr(m));failed=1;}
  else if(m->key_len!=16 || m->len!=100 || !m->key || !m->payload)failed=1;
  else {
   const unsigned char *key=m->key;uint64_t id=0;for(int j=0;j<8;j++)id=(id<<8)|key[j];
   if(id>=128||seen[id])failed=1;
   else {unsigned char expected_key[16],expected_value[104];be64(expected_key,id);be64(expected_key+8,mix(UINT64_C(1592590337)^id));uint64_t state=UINT64_C(1592590337)^(id*UINT64_C(0x9e3779b97f4a7c15));for(int j=0;j<104;j+=8){state=mix(state);be64(expected_value+j,state);}if(memcmp(key,expected_key,16)||memcmp(m->payload,expected_value,100))failed=1;else{seen[id]=1;count++;}}
  }
  rd_kafka_message_destroy(m);
 }
 int64_t low=-1,high=-1;if(rd_kafka_query_watermark_offsets(rk,argv[2],0,&low,&high,10000)||low!=0||high!=128)failed=1;
 if(rd_kafka_consumer_close(rk)) { failed=1; }
 rd_kafka_destroy(rk);int joined=rd_kafka_wait_destroyed(5000)==0;
 printf("{\"status\":\"%s\",\"peer\":\"librdkafka2.15.0\",\"verified\":%d,\"high_watermark\":%lld,\"consumer_closed\":true,\"joined\":%s}\n",(!failed&&count==128&&joined)?"pass":"fail",count,(long long)high,joined?"true":"false");
 return failed||count!=128||!joined;
}
