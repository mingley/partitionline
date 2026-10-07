/* SPDX-License-Identifier: Apache-2.0 */
/* Independent Kafka SDK producer/consumer for the finite codec profile. */
#include "rdkafka.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
static int acked=0,delivery_failed=0;
static const unsigned char text[]={0xe4,0xb8,0x96,0xe7,0x95,0x8c},small[]={0,1,2},a[]={0,255};
static void be64(unsigned char *p,uint64_t n){for(int j=7;j>=0;j--){p[j]=(unsigned char)n;n>>=8;}}
static const void *key(int id,size_t *len){*len=0;if(id==1)return "";if(id==2){*len=3;return "key";}if(id==5){*len=7;return "headers";}return NULL;}
static unsigned char *value(int id,size_t *len){
 *len=id==0?0:id==1?0:id==2?6:id==3?65536:id==4?200000:id==5?3:(size_t)(131071+id-6);
 if(id==0) { return NULL; }
 unsigned char *out=malloc(*len?*len:1);if(!out)return NULL;
 if(id==2)memcpy(out,text,6);else if(id==4)memset(out,'x',*len);else if(id==5)memcpy(out,small,3);
 else {uint64_t state=UINT64_C(0x5eed0001);for(size_t at=0;at<*len;at+=8){state^=state<<13;state^=state>>7;state^=state<<17;for(size_t j=0;j<8&&at+j<*len;j++)out[at+j]=(unsigned char)(state>>(j*8));}}
 return out;
}
static int set(rd_kafka_conf_t *c,const char *k,const char *v){char error[256];if(rd_kafka_conf_set(c,k,v,error,sizeof error)!=RD_KAFKA_CONF_OK){fprintf(stderr,"%s\n",error);return 0;}return 1;}
static void delivery(rd_kafka_t *rk,const rd_kafka_message_t *m,void *opaque){(void)rk;(void)opaque;int id=(int)(uintptr_t)m->_private-1;if(m->err||m->offset!=id)delivery_failed=1;else acked++;}
static int verify(rd_kafka_message_t *m,int id,int64_t timestamp){
 size_t kl,vl;const void *k=key(id,&kl);unsigned char *v=value(id,&vl);
 int good=m->partition==0&&m->offset==id&&rd_kafka_message_timestamp(m,NULL)==timestamp+id&&m->key_len==kl&&m->len==vl;
 if(good)good=(k?(m->key&&(!kl||!memcmp(k,m->key,kl))):!m->key)&&(v?(m->payload&&(!vl||!memcmp(v,m->payload,vl))):!m->payload);
 free(v);rd_kafka_headers_t *headers=NULL;if(rd_kafka_message_headers(m,&headers)||rd_kafka_header_cnt(headers)!=3)return 0;
 const char *names[]={"id","a","nullable"};unsigned char expected_id[8];be64(expected_id,(uint64_t)id);
 for(size_t j=0;j<3&&good;j++){const char *name;const void *data;size_t size;if(rd_kafka_header_get_all(headers,j,&name,&data,&size)||strcmp(name,names[j]))return 0;if(j==0)good=size==8&&data&&!memcmp(data,expected_id,8);else if(j==1)good=size==2&&data&&!memcmp(data,a,2);else good=!data&&size==0;}
 return good;
}
int main(int argc,char **argv){
 if(argc!=6||strcmp(rd_kafka_version_str(),"2.15.0")) { return 2; }
 char *end=NULL;int64_t timestamp=strtoll(argv[5],&end,10);if(!end||*end)return 3;
 int produce=!strcmp(argv[1],"produce");if(!produce&&strcmp(argv[1],"consume"))return 4;
 rd_kafka_conf_t *conf=rd_kafka_conf_new();if(!set(conf,"bootstrap.servers",argv[2])||!set(conf,"socket.timeout.ms","10000"))return 5;
 if(produce){if(!set(conf,"acks","all")||!set(conf,"enable.idempotence","false")||!set(conf,"compression.type",argv[4])||!set(conf,"message.timeout.ms","30000"))return 6;rd_kafka_conf_set_dr_msg_cb(conf,delivery);}
 else if(!set(conf,"group.id","codec-matrix-c")||!set(conf,"enable.auto.commit","false")||!set(conf,"enable.auto.offset.store","false"))return 7;
 char error[256];rd_kafka_t *rk=rd_kafka_new(produce?RD_KAFKA_PRODUCER:RD_KAFKA_CONSUMER,conf,error,sizeof error);if(!rk){fprintf(stderr,"%s\n",error);return 8;}int failed=0,count=0;
 if(produce){for(int id=0;id<9&&!failed;id++){size_t kl,vl;const void *k=key(id,&kl);unsigned char *v=value(id,&vl);unsigned char id_bytes[8];be64(id_bytes,(uint64_t)id);rd_kafka_headers_t *headers=rd_kafka_headers_new(3);
 if(rd_kafka_header_add(headers,"id",-1,id_bytes,8)||rd_kafka_header_add(headers,"a",-1,a,2)||rd_kafka_header_add(headers,"nullable",-1,NULL,0))failed=1;
 if(!failed){rd_kafka_resp_err_t e=rd_kafka_producev(rk,RD_KAFKA_V_TOPIC(argv[3]),RD_KAFKA_V_PARTITION(0),RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),RD_KAFKA_V_KEY(k,kl),RD_KAFKA_V_VALUE(v,vl),RD_KAFKA_V_TIMESTAMP(timestamp+id),RD_KAFKA_V_HEADERS(headers),RD_KAFKA_V_OPAQUE((void *)(uintptr_t)(id+1)),RD_KAFKA_V_END);if(e){fprintf(stderr,"%s\n",rd_kafka_err2str(e));rd_kafka_headers_destroy(headers);failed=1;}}
 else { rd_kafka_headers_destroy(headers); }
 free(v);rd_kafka_poll(rk,0);}
 if(rd_kafka_flush(rk,30000)) { failed=1; }
 count=acked;if(delivery_failed||count!=9)failed=1;
 }else{rd_kafka_poll_set_consumer(rk);rd_kafka_topic_partition_list_t *ps=rd_kafka_topic_partition_list_new(1);rd_kafka_topic_partition_list_add(ps,argv[3],0)->offset=RD_KAFKA_OFFSET_BEGINNING;if(rd_kafka_assign(rk,ps))failed=1;rd_kafka_topic_partition_list_destroy(ps);time_t deadline=time(NULL)+30;
 while(!failed&&count<9&&time(NULL)<deadline){rd_kafka_message_t *m=rd_kafka_consumer_poll(rk,100);if(!m)continue;if(m->err||!verify(m,count,timestamp))failed=1;else count++;rd_kafka_message_destroy(m);}
 int64_t low=-1,high=-1;if(rd_kafka_query_watermark_offsets(rk,argv[3],0,&low,&high,10000)||low!=0||high!=9)failed=1;if(rd_kafka_consumer_close(rk))failed=1;}
 rd_kafka_destroy(rk);int joined=rd_kafka_wait_destroyed(5000)==0;if(!joined)failed=1;
 printf("{\"status\":\"%s\",\"peer\":\"librdkafka2.15.0\",\"mode\":\"%s\",\"records\":%d,\"closed_and_joined\":%s}\n",!failed&&count==9?"pass":"fail",argv[1],count,joined?"true":"false");return failed||count!=9;
}
