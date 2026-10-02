#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>
#include "rdkafka.h"

/* This executable is the producer AND independent direct-assignment consumer.
 * Python only builds provenance/statistics from its JSON; no FFI or wrapper. */
static const char *env(const char *k, const char *d) { const char *v=getenv(k); return v&&*v?v:d; }
static uint64_t num(const char *k, const char *d) {
    char *end; errno=0; unsigned long long n=strtoull(env(k,d),&end,0);
    if(errno || *end || env(k,d)[0]=='-') { fprintf(stderr,"invalid integer: %s\n",k); exit(2); } return n;
}
static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t); return t.tv_sec+t.tv_nsec/1e9; }
static void string(FILE *f,const char *s) {
    fputc('"',f); for(const unsigned char *p=(const unsigned char *)(s?s:"");*p;p++) {
        if(*p=='"'||*p=='\\') fprintf(f,"\\%c",*p);
        else if(*p<32) fprintf(f,"\\u%04x",*p); else fputc(*p,f);
    } fputc('"',f);
}
static void set(rd_kafka_conf_t *c,const char *k,const char *v) {
    char e[512]; if(rd_kafka_conf_set(c,k,v,e,sizeof e)!=RD_KAFKA_CONF_OK) {
        /* Do not print values: a rejected value may contain a credential. */
        fprintf(stderr,"invalid/unsupported librdkafka setting: %s\n",k); exit(2);
    }
}
static rd_kafka_conf_t *config(int consumer) {
    static const char *map[][3]={
      {"ACKS","acks","1"},{"IDEMPOTENT","enable.idempotence","false"},
      {"LINGER_MS","linger.ms","5"},{"BATCH_BYTES","batch.size","1000000"},
      {"BATCH_RECORDS","batch.num.messages","32768"},{"MAX_IN_FLIGHT","max.in.flight.requests.per.connection","5"},
      {"QUEUE_MESSAGES","queue.buffering.max.messages","1000000"},{"QUEUE_KBYTES","queue.buffering.max.kbytes","32768"},
      {"COMPRESSION","compression.codec","none"},{"DELIVERY_TIMEOUT_MS","message.timeout.ms","30000"},
      {"SECURITY_PROTOCOL","security.protocol","plaintext"},{"SASL_MECHANISM","sasl.mechanisms",NULL},
      {"SASL_USERNAME","sasl.username",NULL},{"SASL_PASSWORD","sasl.password",NULL},
      {"TLS_CA_PEM","ssl.ca.location",NULL},{"TLS_CLIENT_CERT_PEM","ssl.certificate.location",NULL},
      {"TLS_CLIENT_KEY_PEM","ssl.key.location",NULL}
    };
    rd_kafka_conf_t *c=rd_kafka_conf_new();
    set(c,"bootstrap.servers",env("KAFKA_BOOTSTRAP","127.0.0.1:9092"));
    set(c,"client.id",consumer?"librdkafka-c-peer-audit":"librdkafka-c-peer");
    set(c,"socket.nagle.disable","true"); set(c,"allow.auto.create.topics","false");
    set(c,"log_level","0");
    for(size_t i=0;i<sizeof(map)/sizeof(map[0]);i++) {
        if(consumer && i<10) continue;
        const char *v=env(map[i][0],map[i][2]); if(v) set(c,map[i][1],v);
    }
    if(!consumer) { set(c,"partitioner","consistent_random"); set(c,"sticky.partitioning.linger.ms","0"); }
    else { set(c,"group.id","librdkafka-c-peer-audit"); set(c,"enable.auto.commit","false");
        set(c,"auto.offset.reset","earliest"); set(c,"isolation.level",env("ISOLATION","read_uncommitted")); }
    return c;
}
static uint64_t mix(uint64_t x) { x+=UINT64_C(0x9e3779b97f4a7c15); x=(x^(x>>30))*UINT64_C(0xbf58476d1ce4e5b9); x=(x^(x>>27))*UINT64_C(0x94d049bb133111eb); return x^(x>>31); }
static void be64(unsigned char *p,uint64_t x) { for(int j=7;j>=0;j--) { p[j]=(unsigned char)x; x>>=8; } }
static uint64_t read64(const unsigned char *p) { uint64_t x=0; for(int j=0;j<8;j++) x=(x<<8)|p[j]; return x; }
static void record(uint64_t id,uint64_t seed,unsigned char *key,unsigned char *value,size_t n,int seeded) {
    be64(key,id); be64(key+8,mix(seed^id));
    if(!seeded) { memset(value,'x',n); return; }
    uint64_t state=seed^(id*UINT64_C(0x9e3779b97f4a7c15));
    for(size_t off=0;off<n;) { unsigned char block[8]; state=mix(state); be64(block,state);
        size_t len=n-off<8?n-off:8; memcpy(value+off,block,len); off+=len; }
}
typedef struct {
    uint64_t offered,accepted,acked,rejected,timed_out,unknown,callback_failures,queue_full;
    uint64_t errors[1024], sample_limit; FILE *samples; int acks; double elapsed;
} phase_t;
typedef struct { phase_t *phase; double started; uint64_t id; } delivery_t;
static void error_count(phase_t *p,rd_kafka_resp_err_t err) {
    int i=(int)err+512; if(i>=0&&i<1024) p->errors[i]++;
}
static void delivered(rd_kafka_t *rk,const rd_kafka_message_t *m,void *opaque) {
    (void)rk; (void)opaque; delivery_t *d=m->_private; phase_t *p=d->phase;
    if(m->err) { p->callback_failures++; error_count(p,m->err);
        if(m->err==RD_KAFKA_RESP_ERR__MSG_TIMED_OUT) p->timed_out++; else p->unknown++;
    } else if(p->acks==0) p->unknown++;
    else { p->acked++; if(p->samples && d->id<p->sample_limit) fprintf(p->samples,"%"PRIu64",%.3f\n",d->id,(now()-d->started)*1e6); }
    free(d);
}
static int produce(rd_kafka_t *rk,const char *topic,uint64_t count,phase_t *p,int partitions,size_t bytes,uint64_t seed,int seeded,int keys,int timeout) {
    unsigned char *value=malloc(bytes?bytes:1),key[16]; if(!value) return 0;
    double begin=now(),deadline=begin+num("RUN_TIMEOUT_MS","120000")/1000.0;
    for(uint64_t i=0;i<count;i++) {
        if(now()>deadline) break;
        p->offered++; record(i,seed,key,value,bytes,seeded);
        delivery_t *d=malloc(sizeof *d); if(!d) { p->rejected++; break; }
        *d=(delivery_t){p,now(),i};
        rd_kafka_resp_err_t err;
        while((err=rd_kafka_producev(rk,RD_KAFKA_V_TOPIC(topic),RD_KAFKA_V_PARTITION((int32_t)(i%(uint64_t)partitions)),
          RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),RD_KAFKA_V_VALUE(value,bytes),
          RD_KAFKA_V_KEY((keys?key:NULL),(keys?sizeof key:0)),RD_KAFKA_V_OPAQUE(d),RD_KAFKA_V_END))==RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            p->queue_full++; rd_kafka_poll(rk,10); if(now()>deadline) break;
        }
        if(err) { free(d); p->rejected++; error_count(p,err); break; }
        p->accepted++; rd_kafka_poll(rk,0);
    }
    rd_kafka_resp_err_t flush=rd_kafka_flush(rk,timeout);
    if(flush) { error_count(p,flush); rd_kafka_purge(rk,RD_KAFKA_PURGE_F_QUEUE|RD_KAFKA_PURGE_F_INFLIGHT); rd_kafka_flush(rk,5000); }
    p->elapsed=now()-begin; free(value);
    return p->acked==count && !p->callback_failures && !flush;
}
static void effective(FILE *f,rd_kafka_t *rk) {
    /* Allowlist only: conf_dump includes passwords/private-key paths. */
    static const char *keys[]={"request.required.acks","enable.idempotence","queue.buffering.max.ms","batch.size","batch.num.messages", "max.in.flight.requests.per.connection","queue.buffering.max.messages","queue.buffering.max.kbytes","compression.codec","message.timeout.ms","security.protocol","sasl.mechanisms","socket.nagle.disable","partitioner","sticky.partitioning.linger.ms","message.send.max.retries","retry.backoff.ms","socket.timeout.ms","connections.max.idle.ms","allow.auto.create.topics","builtin.features"};
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
static int durability(rd_kafka_t *rk,const char *topic,int partitions,int *rf,int *isr,int *nodes) {
    const struct rd_kafka_metadata *m=NULL;
    if(rd_kafka_metadata(rk,1,NULL,&m,10000)) return 0;
    int found=0; *rf=0; *nodes=m->broker_cnt;
    for(int t=0;t<m->topic_cnt;t++) if(!strcmp(m->topics[t].topic,topic) && !m->topics[t].err && m->topics[t].partition_cnt==partitions) {
        found=1; for(int p=0;p<partitions;p++) {
            int replicas=m->topics[t].partitions[p].replica_cnt;
            if(!*rf) { *rf=replicas; }
            if(*rf!=replicas) found=0;
        }
    } rd_kafka_metadata_destroy(m);
    rd_kafka_queue_t *q=rd_kafka_queue_new(rk);
    rd_kafka_ConfigResource_t *res=rd_kafka_ConfigResource_new(RD_KAFKA_RESOURCE_TOPIC,topic);
    rd_kafka_AdminOptions_t *opts=rd_kafka_AdminOptions_new(rk,RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS);
    char err[128]; rd_kafka_AdminOptions_set_request_timeout(opts,10000,err,sizeof err);
    rd_kafka_DescribeConfigs(rk,&res,1,opts,q);
    rd_kafka_event_t *ev=rd_kafka_queue_poll(q,11000); *isr=0;
    if(ev && !rd_kafka_event_error(ev)) {
        size_t n; const rd_kafka_ConfigResource_t **rr=rd_kafka_DescribeConfigs_result_resources(rd_kafka_event_DescribeConfigs_result(ev),&n);
        if(n==1 && !rd_kafka_ConfigResource_error(rr[0])) {
            size_t count; const rd_kafka_ConfigEntry_t **entries=rd_kafka_ConfigResource_configs(rr[0],&count);
            for(size_t i=0;i<count;i++) if(!strcmp(rd_kafka_ConfigEntry_name(entries[i]),"min.insync.replicas")) {
                const char *v=rd_kafka_ConfigEntry_value(entries[i]); if(v) *isr=atoi(v);
            }
        }
    }
    if(ev) { rd_kafka_event_destroy(ev); } rd_kafka_ConfigResource_destroy(res); rd_kafka_AdminOptions_destroy(opts); rd_kafka_queue_destroy(q);
    return found && *rf>0 && *isr>0;
}
static int watermarks(rd_kafka_t *rk,const char *topic,int n,int64_t *offsets) {
    int ok=1; for(int p=0;p<n;p++) { int64_t low=0,high=0;
        if(rd_kafka_query_watermark_offsets(rk,topic,p,&low,&high,10000)) ok=0;
        offsets[p]=high;
    } return ok;
}
static uint64_t consume(rd_kafka_t *rk,const char *topic,int n,int64_t *start,int64_t *end,uint64_t count,size_t bytes,uint64_t seed,int seeded,int keys,uint64_t *duplicates,uint64_t *bad) {
    rd_kafka_topic_partition_list_t *parts=rd_kafka_topic_partition_list_new(n);
    for(int p=0;p<n;p++) rd_kafka_topic_partition_list_add(parts,topic,p)->offset=start[p];
    if(rd_kafka_assign(rk,parts)) { rd_kafka_topic_partition_list_destroy(parts); return 0; }
    rd_kafka_topic_partition_list_destroy(parts);
    unsigned char *seen=calloc(count?count:1,1),*value=malloc(bytes?bytes:1),key[16];
    if(!seen||!value) { free(seen); free(value); return 0; }
    uint64_t verified=0,received=0; double deadline=now()+num("CONSUME_TIMEOUT_MS","30000")/1000.0;
    while(received<count && now()<deadline) {
        rd_kafka_message_t *m=rd_kafka_consumer_poll(rk,100); if(!m) continue;
        if(m->err) { if(m->err!=RD_KAFKA_RESP_ERR__PARTITION_EOF) (*bad)++; rd_kafka_message_destroy(m); continue; }
        if(m->partition<0 || m->partition>=n || m->offset<start[m->partition] || m->offset>=end[m->partition]) { (*bad)++; rd_kafka_message_destroy(m); continue; }
        received++;
        if(keys && m->key_len==16) {
            uint64_t id=read64(m->key);
            if(id>=count) (*bad)++;
            else { record(id,seed,key,value,bytes,seeded);
                if(m->len!=bytes || memcmp(m->key,key,16) || memcmp(m->payload,value,bytes) || id%(uint64_t)n!=(uint64_t)m->partition) (*bad)++;
                else if(seen[id]) (*duplicates)++; else { seen[id]=1; verified++; }
            }
        } else if(keys) (*bad)++;
        else { if(m->len!=bytes) (*bad)++; else { memset(value,'x',bytes); if(memcmp(m->payload,value,bytes)) (*bad)++; } }
        rd_kafka_message_destroy(m);
    }
    free(seen); free(value); return verified;
}
static void phase(FILE *f,phase_t *p) {
    fprintf(f,"{\"offered\":%"PRIu64",\"accepted\":%"PRIu64",\"acknowledged\":%"PRIu64",\"rejected\":%"PRIu64",\"timed_out\":%"PRIu64",\"unknown\":%"PRIu64",\"callback_failures\":%"PRIu64",\"queue_full_retries\":%"PRIu64",\"elapsed_s\":%.9f,\"errors\":[",p->offered,p->accepted,p->acked,p->rejected,p->timed_out,p->unknown,p->callback_failures,p->queue_full,p->elapsed);
    int comma=0; for(int i=0;i<1024;i++) if(p->errors[i]) {
        if(comma++) { fputc(',',f); } fprintf(f,"{\"code\":%d,\"name\":",i-512); string(f,rd_kafka_err2name((rd_kafka_resp_err_t)(i-512))); fprintf(f,",\"count\":%"PRIu64"}",p->errors[i]);
    } fprintf(f,"]}");
}
int main(int argc,char **argv) {
    if(argc!=2 || (strcmp(argv[1],"produce") && strcmp(argv[1],"roundtrip") && strcmp(argv[1],"emit-config"))) { fprintf(stderr,"usage: c-peer produce|roundtrip|emit-config\n"); return 2; }
    int roundtrip=!strcmp(argv[1],"roundtrip"),emit=!strcmp(argv[1],"emit-config");
    uint64_t count=num("COUNT","100000"),warmup=num("WARMUP","10000"),seed=num("RECORD_SEED","0x5EED0001");
    size_t bytes=(size_t)num("PAYLOAD_BYTES","100"); int n=(int)num("PARTITIONS","6"),timeout=(int)num("FLUSH_TIMEOUT_MS","35000");
    int seeded=!strcmp(env("PAYLOAD_MODE","seeded"),"seeded"),keys=!strcmp(env("KEY_MODE","id"),"id");
    if(n<1 || n>10000 || bytes>10000000 || !count || count>1000000000) return 2;
    const char *topic=env("KAFKA_TOPIC","plbench"); char err[512];
    phase_t warm={0},timed={0}; timed.sample_limit=num("LATENCY_SAMPLES","1000000000"); warm.acks=timed.acks=atoi(env("ACKS","1"));
    rd_kafka_conf_t *c=config(0); rd_kafka_conf_set_dr_msg_cb(c,delivered);
    rd_kafka_t *rk=rd_kafka_new(RD_KAFKA_PRODUCER,c,err,sizeof err); if(!rk) { fprintf(stderr,"producer creation failed\n"); return 2; }
    if(strcmp(rd_kafka_version_str(),"2.15.0")) { fprintf(stderr,"library version pin mismatch\n"); rd_kafka_destroy(rk); return 2; }
    if(emit) { effective(stdout,rk); fputc('\n',stdout); rd_kafka_destroy(rk); return 0; }
    timed.samples=fopen(env("C_PEER_SAMPLES","c-peer.samples.csv"),"wx"); if(!timed.samples) { fprintf(stderr,"samples artifact already exists or cannot be created\n"); rd_kafka_destroy(rk); return 2; }
    fprintf(timed.samples,"record_id,latency_us\n");
    rd_kafka_t *audit=NULL; int rf=0,isr=0,nodes=0,durable=0,hwok=0; int64_t *before=calloc((size_t)n,sizeof *before),*after=calloc((size_t)n,sizeof *after);
    if(!before||!after) return 2;
    if(strcmp(env("SKIP_AUDIT","0"),"1")) {
        audit=rd_kafka_new(RD_KAFKA_CONSUMER,config(1),err,sizeof err);
        if(audit) { rd_kafka_poll_set_consumer(audit); durable=durability(audit,topic,n,&rf,&isr,&nodes); }
    }
    int warmok=1; if(warmup) warmok=produce(rk,topic,warmup,&warm,n,bytes,seed,seeded,keys,timeout);
    if(audit) hwok=watermarks(audit,topic,n,before);
    int ok=produce(rk,topic,count,&timed,n,bytes,seed,seeded,keys,timeout) && warmok;
    fclose(timed.samples); timed.samples=NULL;
    uint64_t verified=0,duplicates=0,bad=0; int64_t delta=0;
    if(audit) {
        hwok=watermarks(audit,topic,n,after) && hwok;
        for(int p=0;p<n;p++) { if(after[p]<before[p]) hwok=0; else delta+=after[p]-before[p]; }
        if(roundtrip && ok && hwok) verified=consume(audit,topic,n,before,after,count,bytes,seed,seeded,keys,&duplicates,&bad);
    }
    char *cluster=audit?rd_kafka_clusterid(audit,0):NULL;
    struct rusage ru; getrusage(RUSAGE_SELF,&ru);
    FILE *f=fopen(env("C_PEER_RAW","c-peer.raw.json"),"wx"); if(!f) { fprintf(stderr,"raw artifact already exists or cannot be created\n"); return 2; }
    fprintf(f,"{\"client\":\"librdkafka-native-c\",\"version\":"); string(f,rd_kafka_version_str()); fprintf(f,",\"effective_config\":"); effective(f,rk);
    fprintf(f,",\"warmup\":"); phase(f,&warm); fprintf(f,",\"timed\":"); phase(f,&timed);
    fprintf(f,",\"cluster_id\":"); string(f,cluster);
    fprintf(f,",\"broker_nodes\":%d,\"durability\":{\"verified\":%s,\"replication_factor\":%d,\"min_insync_replicas\":%d},\"high_watermarks\":{\"queried\":%s,\"partitions\":[",nodes,durable?"true":"false",rf,isr,hwok?"true":"false");
    for(int p=0;p<n;p++) fprintf(f,"%s{\"partition\":%d,\"start_offset\":%"PRId64",\"end_offset\":%"PRId64",\"offset_delta\":%"PRId64"}",p?",":"",p,before[p],after[p],after[p]>=before[p]?after[p]-before[p]:0);
    fprintf(f,"],\"total_offset_delta\":%"PRId64"},\"verification\":{\"verified_ids\":%"PRIu64",\"duplicate_ids\":%"PRIu64",\"bad_records\":%"PRIu64",\"performed\":%s},\"resources\":{\"user_cpu_seconds\":%.6f,\"system_cpu_seconds\":%.6f,\"peak_rss_bytes\":%ld,\"threads_count\":%d}}\n",delta,verified,duplicates,bad,roundtrip?"true":"false",ru.ru_utime.tv_sec+ru.ru_utime.tv_usec/1e6,ru.ru_stime.tv_sec+ru.ru_stime.tv_usec/1e6,
#ifdef __APPLE__
      ru.ru_maxrss
#else
      ru.ru_maxrss*1024L
#endif
      ,rd_kafka_thread_cnt()+1
    ); fclose(f); free(before); free(after); if(cluster) rd_kafka_mem_free(audit,cluster);
    if(audit) { rd_kafka_consumer_close(audit); rd_kafka_destroy(audit); }
    rd_kafka_destroy(rk);
    return ok && (!roundtrip || (durable&&hwok&&delta==(int64_t)count&&verified==count&&!duplicates&&!bad)) ? 0:1;
}
