/* Closed-loop, single-outstanding-record comparison driver.
 * Independent Kafka Java readback validates every delivery and byte. */
#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "rdkafka.h"

#define SEED UINT64_C(0x5eed0001)
#define GOLD UINT64_C(0x9e3779b97f4a7c15)
static uint64_t nanos(void) {
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t)) { perror("clock_gettime"); exit(2); }
    return (uint64_t)t.tv_sec * UINT64_C(1000000000) + (uint64_t)t.tv_nsec;
}
static void string(FILE *f,const char *s) {
    fputc('"',f); for(const unsigned char *p=(const unsigned char *)(s?s:"");*p;p++) {
        if(*p=='"'||*p=='\\') fprintf(f,"\\%c",*p);
        else if(*p<32) fprintf(f,"\\u%04x",*p); else fputc(*p,f);
    } fputc('"',f);
}
static void set(rd_kafka_conf_t *c,const char *key,const char *value) {
    char error[512];
    if(rd_kafka_conf_set(c,key,value,error,sizeof(error))!=RD_KAFKA_CONF_OK) {
        fprintf(stderr,"invalid librdkafka setting %s: %s\n",key,error); exit(2);
    }
}
static const char *required(const char *key) {
    const char *value=getenv(key);
    if(!value || !*value) { fprintf(stderr,"missing environment %s\n",key); exit(2); }
    return value;
}
static uint64_t number(const char *key) {
    const char *s=required(key); char *end=NULL; errno=0;
    unsigned long long n=strtoull(s,&end,10);
    if(errno || *s=='-' || !end || *end || !n || n>100000) {
        fprintf(stderr,"invalid %s\n",key); exit(2);
    }
    return (uint64_t)n;
}
static uint64_t mix(uint64_t x) {
    x+=GOLD; x=(x^(x>>30))*UINT64_C(0xbf58476d1ce4e5b9);
    x=(x^(x>>27))*UINT64_C(0x94d049bb133111eb); return x^(x>>31);
}
static void be64(unsigned char *p,uint64_t x) {
    for(int i=7;i>=0;i--) { p[i]=(unsigned char)x; x>>=8; }
}
static void record(uint64_t id,unsigned char key[16],unsigned char value[100]) {
    be64(key,id); be64(key+8,mix(SEED^id));
    uint64_t state=SEED^(id*GOLD);
    for(size_t off=0;off<100;) {
        unsigned char block[8]; state=mix(state); be64(block,state);
        size_t n=100-off<8?100-off:8; memcpy(value+off,block,n); off+=n;
    }
}
typedef struct { uint64_t id,elapsed; int64_t offset; } receipt_t;
typedef struct {
    int done; rd_kafka_resp_err_t error; int32_t partition; int64_t offset; uint64_t end;
} delivery_t;
typedef struct { const char *name; uint64_t expected,count,elapsed; receipt_t *receipts; } phase_t;
static void delivered(rd_kafka_t *rk,const rd_kafka_message_t *m,void *opaque) {
    uint64_t end=nanos();
    (void)rk; (void)opaque;
    delivery_t *d=m->_private;
    d->end=end; d->error=m->err; d->partition=m->partition; d->offset=m->offset; d->done=1;
}
static int phase(rd_kafka_t *rk,const char *topic,uint64_t start,phase_t *p) {
    uint64_t phase_begin=nanos();
    for(uint64_t id=start;id<start+p->expected;id++) {
        unsigned char key[16],value[100]; record(id,key,value);
        delivery_t d={0}; uint64_t begin=nanos();
        rd_kafka_resp_err_t error=rd_kafka_producev(rk,
            RD_KAFKA_V_TOPIC(topic),RD_KAFKA_V_PARTITION(0),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE(value,sizeof value),RD_KAFKA_V_KEY(key,sizeof key),
            RD_KAFKA_V_OPAQUE(&d),RD_KAFKA_V_END);
        if(error) { fprintf(stderr,"%s record %"PRIu64" produce: %s\n",p->name,id,rd_kafka_err2str(error)); p->elapsed=nanos()-phase_begin; return 0; }
        while(!d.done) rd_kafka_poll(rk,10);
        if(d.error || d.partition!=0 || d.offset!=(int64_t)id || d.end<=begin) {
            fprintf(stderr,"%s record %"PRIu64" delivery: %s partition=%d offset=%"PRId64"\n",
                    p->name,id,rd_kafka_err2str(d.error),d.partition,d.offset);
            p->elapsed=nanos()-phase_begin; return 0;
        }
        p->receipts[p->count++]=(receipt_t){id,d.end-begin,d.offset};
    }
    p->elapsed=nanos()-phase_begin; return 1;
}
static void write_phase(FILE *f,const phase_t *p) {
    for(uint64_t i=0;i<p->count;i++) {
        const receipt_t *r=&p->receipts[i];
        fprintf(f,"record,%s,%"PRIu64",0,%"PRId64",%"PRIu64"\n",p->name,r->id,r->offset,r->elapsed);
    }
    fprintf(f,"phase_end,%s,%"PRIu64",%"PRIu64",%d\n",p->name,p->count,p->elapsed,p->count?1:0);
}
static void effective(FILE *f,rd_kafka_t *rk) {
    /* Allowlist only: conf_dump includes passwords/private-key paths. */
    static const char *keys[]={"request.required.acks","enable.idempotence","queue.buffering.max.ms","batch.size","batch.num.messages", "max.in.flight.requests.per.connection","queue.buffering.max.messages","queue.buffering.max.kbytes","compression.codec","message.timeout.ms","request.timeout.ms","message.max.bytes","security.protocol","sasl.mechanisms","socket.nagle.disable","partitioner","sticky.partitioning.linger.ms","message.send.max.retries","retry.backoff.ms","retry.backoff.max.ms","reconnect.backoff.ms","reconnect.backoff.max.ms","socket.timeout.ms","socket.connection.setup.timeout.ms","connections.max.idle.ms","allow.auto.create.topics","builtin.features"};
    rd_kafka_conf_t *copy=rd_kafka_conf_dup(rd_kafka_conf(rk));
    size_t n; const char **dump=rd_kafka_conf_dump(copy,&n); int comma=0;
    fputc('{',f); for(size_t i=0;i<n;i+=2) for(size_t j=0;j<sizeof keys/sizeof keys[0];j++) if(!strcmp(dump[i],keys[j])) {
        if(comma++) { fputc(',',f); } string(f,dump[i]); fputc(':',f); string(f,dump[i+1]);
    }
    rd_kafka_conf_dump_free(dump,n);
    rd_kafka_topic_conf_t *tc=rd_kafka_default_topic_conf_dup(rk);
    dump=rd_kafka_topic_conf_dump(tc,&n);
    for(size_t i=0;i<n;i+=2) for(size_t j=0;j<sizeof keys/sizeof keys[0];j++) if(!strcmp(dump[i],keys[j]) && strcmp(dump[i+1],"inherit")) {
        if(comma++) { fputc(',',f); } string(f,dump[i]); fputc(':',f); string(f,dump[i+1]);
    }
    fputc('}',f); rd_kafka_conf_dump_free(dump,n); rd_kafka_topic_conf_destroy(tc); rd_kafka_conf_destroy(copy);
}

int main(void) {
    if(strcmp(rd_kafka_version_str(),"2.15.0")) { fprintf(stderr,"unexpected librdkafka %s\n",rd_kafka_version_str()); return 2; }
    uint64_t warmup=number("WARMUP"),count=number("COUNT"),bytes=number("PAYLOAD_BYTES");
    if(bytes!=100) { fprintf(stderr,"payload must be 100 bytes\n"); return 2; }
    const char *topic=required("KAFKA_TOPIC"),*trace=required("ACK_TRACE"),*config_path=required("C_CONFIG");
    rd_kafka_conf_t *cfg=rd_kafka_conf_new();
    static const char *pairs[][2]={
        {"client.id","paired-ack-benchmark"},{"acks","all"},{"enable.idempotence","true"},
        {"linger.ms","0"},{"batch.size","1000000"},{"batch.num.messages","1"},
        {"max.in.flight.requests.per.connection","1"},{"queue.buffering.max.messages","1000000"},
        {"queue.buffering.max.kbytes","32768"},{"compression.codec","none"},
        {"message.timeout.ms","30000"},{"request.timeout.ms","30000"},{"message.max.bytes","1048576"},
        {"socket.timeout.ms","30000"},{"socket.connection.setup.timeout.ms","10000"},
        {"connections.max.idle.ms","540000"},{"socket.nagle.disable","true"},
        {"security.protocol","plaintext"},{"allow.auto.create.topics","false"},{"log_level","0"}
    };
    set(cfg,"bootstrap.servers",required("KAFKA_BOOTSTRAP"));
    for(size_t i=0;i<sizeof pairs/sizeof pairs[0];i++) set(cfg,pairs[i][0],pairs[i][1]);
    rd_kafka_conf_set_dr_msg_cb(cfg,delivered);
    char error[512]; rd_kafka_t *rk=rd_kafka_new(RD_KAFKA_PRODUCER,cfg,error,sizeof(error));
    if(!rk) { fprintf(stderr,"rd_kafka_new: %s\n",error); return 2; }
    FILE *cf=fopen(config_path,"wx");
    if(!cf) { perror("config output"); rd_kafka_destroy(rk); return 2; }
    effective(cf,rk); fputc('\n',cf); if(fclose(cf)) { perror("config close"); rd_kafka_destroy(rk); return 2; }
    phase_t warm={"warmup",warmup,0,0,calloc((size_t)warmup,sizeof(receipt_t))};
    phase_t timed={"measure",count,0,0,calloc((size_t)count,sizeof(receipt_t))};
    if(!warm.receipts || !timed.receipts) { fprintf(stderr,"allocation failed\n"); rd_kafka_destroy(rk); return 2; }
    int success=phase(rk,topic,0,&warm) && phase(rk,topic,warmup,&timed);
    if(success && rd_kafka_flush(rk,30000)!=RD_KAFKA_RESP_ERR_NO_ERROR) success=0;
    FILE *out=fopen(trace,"wx");
    if(!out) { perror("trace output"); success=0; }
    else {
        fprintf(out,"config,%"PRIu64",%"PRIu64",%"PRIu64",1,100\n",SEED,warmup,count);
        fprintf(out,"settings,-1,true,0,1000000,1,1,1,33554432,30000,1048576,540000\n");
        write_phase(out,&warm); write_phase(out,&timed);
        fprintf(out,"%s\n",success?"complete":"failed,delivery");
        if(fclose(out)) { perror("trace close"); success=0; }
    }
    free(warm.receipts); free(timed.receipts); rd_kafka_destroy(rk);
    return success?0:1;
}
