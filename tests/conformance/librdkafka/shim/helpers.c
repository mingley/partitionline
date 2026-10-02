#define _POSIX_C_SOURCE 200809L
#include "test.h"
#undef rd_kafka_flush
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static FILE *events;
static int failures,phase_count,delivered,callback_errors;
static int *active_remains;
static const char *topic;
static double monotonic(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+t.tv_nsec/1e9; }
static const char *setting(const char *key,const char *fallback) { const char *v=getenv(key); return v&&*v?v:fallback; }
static void string(const char *s) { fputc('"',events); for(const unsigned char *p=(const unsigned char *)s;*p;p++) { if(*p=='"'||*p=='\\') fprintf(events,"\\%c",*p); else if(*p<32) fprintf(events,"\\u%04x",*p); else fputc(*p,events); } fputc('"',events); }
void fixture_assert(int condition,const char *assertion,int line) {
    fprintf(events,"{\"event\":\"assertion\",\"line\":%d,\"passed\":%s,\"assertion\":",line,condition?"true":"false"); string(assertion); fprintf(events,"}\n"); fflush(events);
    if(!condition) failures++;
}
void fixture_timing_start(test_timing_t *timer,const char *name) { timer->name=name; timer->start=monotonic(); }
void fixture_timing_assert(test_timing_t *timer,double lower,double upper) {
    double elapsed=(monotonic()-timer->start)*1000;
    int ok=elapsed>=lower&&elapsed<=upper;
    fprintf(events,"{\"event\":\"timing_assertion\",\"name\":"); string(timer->name);
    fprintf(events,",\"elapsed_ms\":%.6f,\"lower_ms\":%.0f,\"upper_ms\":%.0f,\"passed\":%s}\n",elapsed,lower,upper,ok?"true":"false"); fflush(events);
    if(!ok) failures++;
}
const char *test_mk_topic_name(const char *base,int unique) { (void)base; (void)unique; return topic; }
void test_conf_set(rd_kafka_conf_t *conf,const char *key,const char *value) {
    char err[512]; if(rd_kafka_conf_set(conf,key,value,err,sizeof err)!=RD_KAFKA_CONF_OK) { fprintf(stderr,"invalid conf %s\n",key); exit(2); }
}
void test_conf_init(rd_kafka_conf_t **conf,void *topic_conf,int seconds) {
    (void)topic_conf; (void)seconds;
    *conf=rd_kafka_conf_new();
    test_conf_set(*conf,"bootstrap.servers",setting("KAFKA_BOOTSTRAP","127.0.0.1:19104"));
    test_conf_set(*conf,"acks","1"); test_conf_set(*conf,"enable.idempotence","false");
    test_conf_set(*conf,"max.in.flight.requests.per.connection","1");
    test_conf_set(*conf,"batch.size","1000000"); test_conf_set(*conf,"batch.num.messages","32768");
    test_conf_set(*conf,"queue.buffering.max.kbytes","32768"); test_conf_set(*conf,"queue.buffering.max.messages","1000000");
    test_conf_set(*conf,"message.timeout.ms","30000"); test_conf_set(*conf,"socket.nagle.disable","true");
    test_conf_set(*conf,"allow.auto.create.topics","false"); test_conf_set(*conf,"compression.codec","none");
    test_conf_set(*conf,"security.protocol","plaintext"); test_conf_set(*conf,"log_level","0");
}
rd_kafka_t *test_create_handle(rd_kafka_type_t type,rd_kafka_conf_t *conf) {
    char err[512]; rd_kafka_t *rk=rd_kafka_new(type,conf,err,sizeof err); if(!rk) { fprintf(stderr,"handle creation failed\n"); exit(2); } return rk;
}
void test_create_topic_wait_exists(rd_kafka_t *rk,const char *name,int parts,int rf,int timeout) {
    /* Lifecycle moved to orchestrator; this helper verifies the fresh topic. */
    const struct rd_kafka_metadata *meta=NULL;
    int found=0;
    if(!rd_kafka_metadata(rk,1,NULL,&meta,timeout)) {
        for(int t=0;t<meta->topic_cnt;t++) if(!strcmp(meta->topics[t].topic,name)&&!meta->topics[t].err&&meta->topics[t].partition_cnt==parts) {
            found=1; for(int p=0;p<parts;p++) if(meta->topics[t].partitions[p].replica_cnt!=rf) found=0;
        } rd_kafka_metadata_destroy(meta);
    }
    fixture_assert(found,"fresh topic metadata: one partition, RF1",__LINE__);
    if(!found) exit(2);
}
typedef struct { int *remains; int id; } delivery_t;
static void key_value(int id,char *key,char *value) {
    snprintf(key,16,"m-%03d",id); memset(value,'x',50); char prefix[5]; snprintf(prefix,sizeof prefix,"%03d:",id); memcpy(value,prefix,4); value[50]='\0';
}
void test_dr_msg_cb(rd_kafka_t *rk,const rd_kafka_message_t *message,void *opaque) {
    (void)rk; (void)opaque; delivery_t *d=message->_private; (*d->remains)--; delivered++; if(message->err) callback_errors++;
    char key[16],value[51]; key_value(d->id,key,value);
    fprintf(events,"{\"event\":\"delivery\",\"id\":"); string(key); fprintf(events,",\"payload\":"); string(value);
    fprintf(events,",\"partition\":%d,\"offset\":%"PRId64",\"error_code\":%d}\n",message->partition,message->offset,(int)message->err);
    fflush(events); free(d);
}
void test_produce_msgs2_nowait(rd_kafka_t *rk,const char *name,uint64_t testid,int partition,int first,int count,const void *payload,size_t size,int *remains) {
    (void)testid; (void)first; (void)payload;
    fixture_assert(count==50&&partition==0&&size==50,"upstream two batches of 50 records, one partition, 50-byte values",__LINE__);
    *remains=0; active_remains=remains;
    fprintf(events,"{\"event\":\"enqueue_phase\",\"phase\":%d,\"count\":%d,\"linger_ms\":10000}\n",phase_count,count);
    for(int i=0;i<count;i++) {
        int id=phase_count*50+i; char key[16],value[51]; key_value(id,key,value);
        delivery_t *d=malloc(sizeof *d); if(!d) exit(2); *d=(delivery_t){remains,id};
        rd_kafka_resp_err_t err=rd_kafka_producev(rk,RD_KAFKA_V_TOPIC(name),RD_KAFKA_V_PARTITION(partition),RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_KEY(key,strlen(key)),RD_KAFKA_V_VALUE(value,size),RD_KAFKA_V_OPAQUE(d),RD_KAFKA_V_END);
        fixture_assert(!err,"enqueue accepted",__LINE__); if(err) { free(d); continue; } (*remains)++;
    } phase_count++;
}
rd_kafka_resp_err_t fixture_flush(rd_kafka_t *rk,int timeout) {
    int mutant=!strcmp(setting("VARIANT","normal"),"omit-flush");
    fprintf(events,"{\"event\":\"flush_call\",\"timeout_ms\":%d,\"omitted\":%s}\n",timeout,mutant?"true":"false"); fflush(events);
    if(mutant) { while(*active_remains>0) rd_kafka_poll(rk,1000); return RD_KAFKA_RESP_ERR_NO_ERROR; }
    return rd_kafka_flush(rk,timeout);
}
static void audit_consume(const char *name,int count) {
    rd_kafka_conf_t *conf=rd_kafka_conf_new();
    test_conf_set(conf,"bootstrap.servers",setting("KAFKA_BOOTSTRAP","127.0.0.1:19104"));
    test_conf_set(conf,"group.id","kl01-13-independent-audit"); test_conf_set(conf,"enable.auto.commit","false");
    test_conf_set(conf,"isolation.level","read_uncommitted"); test_conf_set(conf,"allow.auto.create.topics","false"); test_conf_set(conf,"log_level","0");
    rd_kafka_t *c=test_create_handle(RD_KAFKA_CONSUMER,conf); rd_kafka_poll_set_consumer(c);
    int64_t low=0,high=0; fixture_assert(!rd_kafka_query_watermark_offsets(c,name,0,&low,&high,5000),"independent ListOffsets query succeeds",__LINE__);
    fprintf(events,"{\"event\":\"high_watermark\",\"low\":%"PRId64",\"high\":%"PRId64"}\n",low,high);
    rd_kafka_topic_partition_list_t *assign=rd_kafka_topic_partition_list_new(1); rd_kafka_topic_partition_list_add(assign,name,0)->offset=0;
    fixture_assert(!rd_kafka_assign(c,assign),"independent consumer assignment offset0",__LINE__); rd_kafka_topic_partition_list_destroy(assign);
    int received=0,seen[100]={0}; double deadline=monotonic()+15;
    while(received<count&&monotonic()<deadline) {
        rd_kafka_message_t *m=rd_kafka_consumer_poll(c,100); if(!m) continue;
        if(m->err) { rd_kafka_message_destroy(m); continue; }
        int id=-1;
        if(m->key_len==5&&m->len==50) { char key[6]; memcpy(key,m->key,5); key[5]='\0'; if(sscanf(key,"m-%d",&id)!=1) id=-1; }
        int valid=id>=0&&id<100; char key[16],value[51];
        if(valid) { key_value(id,key,value); valid=!seen[id]&&m->partition==0&&m->offset==id&&!memcmp(key,m->key,5)&&!memcmp(value,m->payload,50); }
        fixture_assert(valid,"independent ID/payload/partition/offset verification",__LINE__);
        if(valid) { seen[id]=1; fprintf(events,"{\"event\":\"consumed\",\"id\":"); string(key); fprintf(events,",\"payload\":"); string(value); fprintf(events,",\"partition\":0,\"offset\":%"PRId64"}\n",m->offset); }
        received++; rd_kafka_message_destroy(m);
    }
    fixture_assert(received==count&&high==count,"upstream receipt count and independent HW match expected100",__LINE__);
    rd_kafka_consumer_close(c); rd_kafka_destroy(c);
}
void test_consume_msgs_easy(const char *name,const char *group,int partition,int eof,int count,void *opaque) { (void)group; (void)partition; (void)eof; (void)opaque; audit_consume(name,count); }
int main_0125_immediate_flush(int,char **);
int main(int argc,char **argv) {
    const char *path=setting("EVENTS_PATH","0125-events.jsonl"); events=fopen(path,"wx"); if(!events) { fprintf(stderr,"events exists or cannot be created\n"); return 2; }
    topic=setting("KAFKA_TOPIC","kl01-13-0125");
    fprintf(events,"{\"event\":\"start\",\"peer\":\"librdkafka\",\"version\":\"%s\",\"topic\":",rd_kafka_version_str()); string(topic); fprintf(events,",\"variant\":"); string(setting("VARIANT","normal")); fprintf(events,"}\n");
    if(argc>1&&!strcmp(argv[1],"audit")) audit_consume(topic,100);
    else { fixture_assert(!strcmp(rd_kafka_version_str(),"2.15.0"),"source pinned librdkafka2.15.0 loaded",__LINE__); main_0125_immediate_flush(argc,argv); fixture_assert(delivered==100&&!callback_errors,"all100 delivery callbacks successful",__LINE__); }
    fprintf(events,"{\"event\":\"result\",\"assertion_failures\":%d,\"delivery_callbacks\":%d,\"callback_errors\":%d}\n",failures,delivered,callback_errors); fclose(events);
    return failures?1:0;
}
