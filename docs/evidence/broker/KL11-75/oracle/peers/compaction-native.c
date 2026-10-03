/* Genuine pinned librdkafka public Produce/manual sparse Fetch restart peer. */
#define _POSIX_C_SOURCE 200809L
#include <rdkafka.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static const char *scenarios[] = {"mixed", "removed", "nulls"};
static const char *keys[3][10] = {
    {"a",NULL,"a","b","b","",NULL,"c","a","a"},
    {"a","a","a","a","a"}, {NULL,NULL,"z","z"}
};
static const char *values[3][10] = {
    {"o","v","n","o",NULL,"",NULL,NULL,"p","q"},
    {"o","n",NULL,"p","q"}, {"v",NULL,"p","q"}
};
static const int64_t times[3][10] = {
    {1000,1007,1003,1007,1010,1011,1012,1013,1014,1015},
    {1000,1001,1002,1003,1004}, {1000,1001,1002,1003}
};
static const int ends[] = {9,4,3};
static unsigned assertions, records, deliveries;
static const char *bootstrap, *prefix;
static FILE *output;
static int first_event = 1;
typedef struct { int scenario; int offset; int delivered; } delivery_t;
static delivery_t acknowledgments[3][10];

static void check(int condition, const char *label) {
    assertions++;
    if (!condition) { fprintf(stderr,"assertion failed: %s\n",label); exit(1); }
}
static int64_t now_ms(void) {
    struct timespec value; check(clock_gettime(CLOCK_MONOTONIC,&value)==0,"bounded monotonic clock");
    return (int64_t)value.tv_sec*1000+value.tv_nsec/1000000;
}
static void topic(char out[64], int scenario) {
    int size=snprintf(out,64,"%s-%s",prefix,scenarios[scenario]); check(size>0 && size<64,"bounded topic name");
}
static void set(rd_kafka_conf_t *config,const char *key,const char *value) {
    char error[512]; check(rd_kafka_conf_set(config,key,value,error,sizeof(error))==RD_KAFKA_CONF_OK,key);
}
static rd_kafka_t *client(int consumer);
static void event(void) { if (!first_event) fputc(',',output); first_event=0; }
static void hex(const void *data,size_t length) {
    if (!data) { fputs("null",output); return; }
    const unsigned char *bytes=data; fputc('"',output);
    for(size_t i=0;i<length;i++) fprintf(output,"%02x",bytes[i]);
    fputc('"',output);
}
static void receipt(const char *name,int64_t offset,int64_t time,const void *key,size_t key_len,const void *value,size_t value_len,rd_kafka_headers_t *headers) {
    fprintf(output,"{\"topic\":\"%s\",\"partition\":0,\"offset\":%" PRId64 ",\"timestamp\":%" PRId64 ",\"key_hex\":",name,offset,time);
    hex(key,key_len); fputs(",\"value_hex\":",output); hex(value,value_len); fputs(",\"headers\":[",output);
    for(size_t i=0;i<3;i++) {
        const char *hname; const void *hvalue; size_t length;
        check(rd_kafka_header_get_all(headers,i,&hname,&hvalue,&length)==RD_KAFKA_RESP_ERR_NO_ERROR,"public header output");
        fprintf(output,"%s{\"key\":\"%s\",\"value_hex\":",i?",":"",hname); hex(hvalue,length); fputc('}',output);
    }
    fputs("]}",output);
}
static rd_kafka_headers_t *new_headers(void) {
    rd_kafka_headers_t *headers=rd_kafka_headers_new(3); check(headers!=NULL,"bounded headers");
    check(rd_kafka_header_add(headers,"d",-1,"a",1)==RD_KAFKA_RESP_ERR_NO_ERROR,"first duplicate header");
    check(rd_kafka_header_add(headers,"d",-1,NULL,0)==RD_KAFKA_RESP_ERR_NO_ERROR,"null duplicate header");
    check(rd_kafka_header_add(headers,"e",-1,"",0)==RD_KAFKA_RESP_ERR_NO_ERROR,"empty header"); return headers;
}
static void delivery(rd_kafka_t *handle,const rd_kafka_message_t *message,void *opaque) {
    (void)handle; (void)opaque; delivery_t *ack=message->_private;
    check(ack!=NULL && !ack->delivered && message->err==RD_KAFKA_RESP_ERR_NO_ERROR,"unique successful public delivery");
    check(message->partition==0 && message->offset==ack->offset,"actual public Producer offset/partition");
    ack->delivered=1; deliveries++; records++; char name[64]; topic(name,ack->scenario);
    rd_kafka_headers_t *headers=new_headers(); event(); fputs("{\"label\":\"public-producer-delivery\",\"record\":",output);
    const char *key=keys[ack->scenario][ack->offset],*value=values[ack->scenario][ack->offset];
    receipt(name,message->offset,times[ack->scenario][ack->offset],key,key?strlen(key):0,value,value?strlen(value):0,headers);
    fputc('}',output); rd_kafka_headers_destroy(headers);
}
static rd_kafka_t *client(int consumer) {
    rd_kafka_conf_t *config=rd_kafka_conf_new(); check(config!=NULL,"client configuration");
    set(config,"bootstrap.servers",bootstrap); set(config,"client.id","compaction-public-native");
    set(config,"allow.auto.create.topics","false"); set(config,"socket.timeout.ms","5000");
    if(consumer) {
        set(config,"group.id","compaction-manual"); set(config,"enable.auto.commit","false"); set(config,"enable.auto.offset.store","false");
        set(config,"auto.offset.reset","error"); set(config,"fetch.min.bytes","1"); set(config,"fetch.wait.max.ms","20");
        set(config,"message.max.bytes","65536"); set(config,"fetch.max.bytes","65536"); set(config,"fetch.message.max.bytes","4096");
        set(config,"isolation.level","read_uncommitted"); set(config,"enable.partition.eof","true");
    } else {
        set(config,"enable.idempotence","false"); set(config,"acks","1"); set(config,"message.send.max.retries","0");
        set(config,"max.in.flight.requests.per.connection","1"); set(config,"compression.type","none"); set(config,"linger.ms","0");
        set(config,"batch.num.messages","1"); set(config,"batch.size","128"); set(config,"queue.buffering.max.kbytes","1024");
        set(config,"message.timeout.ms","15000"); set(config,"request.timeout.ms","5000"); rd_kafka_conf_set_dr_msg_cb(config,delivery);
    }
    char error[512]; rd_kafka_t *handle=rd_kafka_new(consumer?RD_KAFKA_CONSUMER:RD_KAFKA_PRODUCER,config,error,sizeof(error));
    if(!handle) fprintf(stderr,"client construction: %s\n",error);
    check(handle!=NULL,"actual public native client"); return handle;
}
static void create(rd_kafka_t *handle) {
    rd_kafka_NewTopic_t *input[3]; char error[512];
    for(int i=0;i<3;i++) { char name[64]; topic(name,i); input[i]=rd_kafka_NewTopic_new(name,1,1,error,sizeof(error)); check(input[i]!=NULL,"new topic"); }
    rd_kafka_queue_t *queue=rd_kafka_queue_new(handle); check(queue!=NULL,"admin queue");
    rd_kafka_CreateTopics(handle,input,3,NULL,queue); rd_kafka_event_t *result=rd_kafka_queue_poll(queue,10000);
    check(result!=NULL && rd_kafka_event_type(result)==RD_KAFKA_EVENT_CREATETOPICS_RESULT,"public CreateTopics event");
    check(rd_kafka_event_error(result)==RD_KAFKA_RESP_ERR_NO_ERROR,"public admin envelope");
    size_t count=0; const rd_kafka_topic_result_t **rows=rd_kafka_CreateTopics_result_topics(rd_kafka_event_CreateTopics_result(result),&count);
    check(count==3,"exact native topic count"); for(size_t i=0;i<count;i++) check(rd_kafka_topic_result_error(rows[i])==RD_KAFKA_RESP_ERR_NO_ERROR,"public topic creation");
    rd_kafka_event_destroy(result); rd_kafka_queue_destroy(queue); for(int i=0;i<3;i++) rd_kafka_NewTopic_destroy(input[i]);
}
static void produce(int seed) {
    rd_kafka_t *handle=client(0); if(seed) create(handle); unsigned expected=0;
    for(int s=0;s<3;s++) {
        char name[64]; topic(name,s); int start=seed?0:ends[s],end=seed?ends[s]:start+1;
        for(int i=start;i<end;i++) {
            delivery_t *ack=&acknowledgments[s][i]; ack->scenario=s; ack->offset=i;
            rd_kafka_headers_t *headers=new_headers(); const char *key=keys[s][i],*value=values[s][i];
            rd_kafka_resp_err_t error=rd_kafka_producev(handle,RD_KAFKA_V_TOPIC(name),RD_KAFKA_V_PARTITION(0),RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                RD_KAFKA_V_KEY(key,key?strlen(key):0),RD_KAFKA_V_VALUE((void *)value,value?strlen(value):0),RD_KAFKA_V_HEADERS(headers),
                RD_KAFKA_V_TIMESTAMP(times[s][i]),RD_KAFKA_V_OPAQUE(ack),RD_KAFKA_V_END);
            if(error!=RD_KAFKA_RESP_ERR_NO_ERROR) rd_kafka_headers_destroy(headers);
            check(error==RD_KAFKA_RESP_ERR_NO_ERROR,"actual native Produce enqueue");
            check(rd_kafka_flush(handle,15000)==RD_KAFKA_RESP_ERR_NO_ERROR && ack->delivered,"serial one-record public batch flush"); expected++;
        }
    }
    check(deliveries==expected,"every public delivery callback observed"); rd_kafka_destroy(handle);
}
static int retained(int s,int offset,const char *stage) {
    if(strcmp(stage,"initial")==0) return offset<ends[s];
    int first=strcmp(stage,"first")==0 || strcmp(stage,"before-expiry")==0;
    int appended=strcmp(stage,"appended")==0;
    if(s==0) return offset==2 || offset==5 || offset==8 || (first && (offset==4 || offset==7)) || (appended && offset==9);
    if(s==1) return offset==3 || (first && offset==2) || (appended && offset==4);
    return offset==2 || (appended && offset==3);
}
static void same(const void *actual,size_t length,const char *expected,const char *label) {
    check((actual==NULL)==(expected==NULL) && length==(expected?strlen(expected):0),label);
    if(length) check(memcmp(actual,expected,length)==0,label);
}
static int64_t position(rd_kafka_t *handle,const char *name) {
    rd_kafka_topic_partition_list_t *list=rd_kafka_topic_partition_list_new(1); check(list!=NULL,"position list");
    rd_kafka_topic_partition_t *row=rd_kafka_topic_partition_list_add(list,name,0);
    check(rd_kafka_position(handle,list)==RD_KAFKA_RESP_ERR_NO_ERROR && row->err==RD_KAFKA_RESP_ERR_NO_ERROR,"actual public consumer position");
    int64_t value=row->offset; rd_kafka_topic_partition_list_destroy(list); return value;
}
static void consume(const char *stage) {
    rd_kafka_t *handle=client(1); const struct rd_kafka_metadata *metadata=NULL;
    check(rd_kafka_metadata(handle,1,NULL,&metadata,5000)==RD_KAFKA_RESP_ERR_NO_ERROR && metadata && metadata->broker_cnt==1,"unchanged native all-topics query");
    rd_kafka_metadata_destroy(metadata); check(rd_kafka_poll_set_consumer(handle)==RD_KAFKA_RESP_ERR_NO_ERROR,"consumer poll routing");
    for(int s=0;s<3;s++) {
        char name[64]; topic(name,s); int end=ends[s]+(strcmp(stage,"appended")==0);
        int64_t low,high; check(rd_kafka_query_watermark_offsets(handle,name,0,&low,&high,5000)==RD_KAFKA_RESP_ERR_NO_ERROR && low==0 && high==end,"unchanged public logical floor/LEO");
        int starts[]={0,s==0?3:1,end};
        for(int round=0;round<3;round++) {
            int start=starts[round],next=start; while(next<end && !retained(s,next,stage)) next++;
            rd_kafka_topic_partition_list_t *assignment=rd_kafka_topic_partition_list_new(1); check(assignment!=NULL,"manual assignment");
            rd_kafka_topic_partition_list_add(assignment,name,0)->offset=start;
            check(rd_kafka_assign(handle,assignment)==RD_KAFKA_RESP_ERR_NO_ERROR,"public manual assignment/seek");
            check(rd_kafka_seek_partitions(handle,assignment,5000)==RD_KAFKA_RESP_ERR_NO_ERROR && assignment->elems[0].err==RD_KAFKA_RESP_ERR_NO_ERROR,"actual public seek");
            rd_kafka_topic_partition_list_destroy(assignment); int64_t deadline=now_ms()+10000;
            int eof_seen=0; int64_t eof_offset=RD_KAFKA_OFFSET_INVALID; unsigned received=0;
            int64_t last_delivered_offset=RD_KAFKA_OFFSET_INVALID;
            while((next<end || !eof_seen) && now_ms()<deadline) {
                rd_kafka_message_t *record=rd_kafka_consumer_poll(handle,100); if(!record) continue;
                if(record->err==RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    check(!eof_seen && next==end && record->rkt!=NULL && record->partition==0 && record->offset==end && strcmp(rd_kafka_topic_name(record->rkt),name)==0,"actual native EOF matches preserved LEO after exact records");
                    eof_seen=1; eof_offset=record->offset;
                    check(rd_kafka_query_watermark_offsets(handle,name,0,&low,&high,5000)==RD_KAFKA_RESP_ERR_NO_ERROR && low==0 && high==end,"public watermark independently matches native EOF");
                    int64_t actual_position=position(handle,name);
                    check(actual_position==(received?last_delivered_offset+1:RD_KAFKA_OFFSET_INVALID),"native position follows last consumed message or remains invalid after empty seek");
                    event(); fprintf(output,"{\"label\":\"public-consumer-eof\",\"topic\":\"%s\",\"partition\":%d,\"seek\":%d,\"eof_offset\":%" PRId64 ",\"position\":%" PRId64 ",\"records_since_seek\":%u,\"beginning_offset\":%" PRId64 ",\"end_offset\":%" PRId64 "}",name,record->partition,start,eof_offset,actual_position,received,low,high);
                    rd_kafka_message_destroy(record); continue;
                }
                check(record->err==RD_KAFKA_RESP_ERR_NO_ERROR && next<end && record->partition==0 && record->offset==next && strcmp(rd_kafka_topic_name(record->rkt),name)==0,"actual native seek skips holes");
                rd_kafka_timestamp_type_t type; int64_t time=rd_kafka_message_timestamp(record,&type);
                check(type==RD_KAFKA_TIMESTAMP_CREATE_TIME && time==times[s][next],"exact native CreateTime");
                same(record->key,record->key_len,keys[s][next],"exact native key/null/empty"); same(record->payload,record->len,values[s][next],"exact native value/null/empty");
                rd_kafka_headers_t *headers=NULL; check(rd_kafka_message_headers(record,&headers)==RD_KAFKA_RESP_ERR_NO_ERROR && rd_kafka_header_cnt(headers)==3,"ordered native headers");
                for(size_t i=0;i<3;i++) { const char *hname; const void *hvalue; size_t length;
                    check(rd_kafka_header_get_all(headers,i,&hname,&hvalue,&length)==RD_KAFKA_RESP_ERR_NO_ERROR,"native header access");
                    check(strcmp(hname,i==2?"e":"d")==0,"header name/order"); same(hvalue,length,i==0?"a":i==1?NULL:"","exact header bytes"); }
                event(); fprintf(output,"{\"label\":\"public-consumer-record\",\"seek\":%d,\"stage\":\"%s\",\"record\":",start,stage);
                receipt(name,record->offset,time,record->key,record->key_len,record->payload,record->len,headers); fputc('}',output); records++; received++; last_delivered_offset=record->offset;
                rd_kafka_message_destroy(record); next++; while(next<end && !retained(s,next,stage)) next++;
            }
            int64_t actual_position=position(handle,name);
            check(next==end && eof_seen && eof_offset==end && actual_position==(received?last_delivered_offset+1:RD_KAFKA_OFFSET_INVALID),"native consumer completes at actual EOF with SDK-specific position");
            check(start!=end || received==0,"exact-end native seek returns no records"); event();
            fprintf(output,"{\"label\":\"public-consumer-position\",\"topic\":\"%s\",\"seek\":%d,\"position\":%" PRId64 ",\"position_semantics\":\"last-consumed-plus-one-or-invalid\",\"eof_offset\":%" PRId64 ",\"records_since_seek\":%u,\"beginning_offset\":%" PRId64 ",\"end_offset\":%" PRId64 "}",name,start,actual_position,eof_offset,received,low,high);
        }
    }
    check(rd_kafka_consumer_close(handle)==RD_KAFKA_RESP_ERR_NO_ERROR,"bounded native close"); rd_kafka_destroy(handle);
}
int main(int argc,char **argv) {
    if(argc!=6) { fprintf(stderr,"bootstrap prefix seed|read|append stage output-json\n"); return 2; }
    bootstrap=argv[1]; prefix=argv[2]; const char *operation=argv[3],*stage=argv[4];
    check(strlen(bootstrap)<=128 && strlen(prefix)>0 && strlen(prefix)<=40 && strspn(prefix,"abcdefghijklmnopqrstuvwxyz0123456789-")==strlen(prefix),"bounded trusted native arguments");
    check(strcmp(stage,"initial")==0 || strcmp(stage,"first")==0 || strcmp(stage,"before-expiry")==0 || strcmp(stage,"expired")==0 || strcmp(stage,"restart")==0 || strcmp(stage,"appended")==0,"bounded stage");
    output=fopen(argv[5],"w"); check(output!=NULL,"receipt output");
    fprintf(output,"{\"schema_version\":1,\"peer\":\"native-c\",\"release\":\"2.15.0\",\"operation\":\"%s\",\"stage\":\"%s\",\"prefix\":\"%s\",\"history\":[",operation,stage,prefix);
    if(strcmp(operation,"seed")==0) produce(1); else if(strcmp(operation,"append")==0) produce(0); else if(strcmp(operation,"read")==0) consume(stage); else check(0,"operation");
    fprintf(output,"],\"assertions\":%u,\"records\":%u,\"identities\":{},\"passed\":true}\n",assertions,records);
    check(fclose(output)==0,"receipt close"); return 0;
}
