#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

int main(void){uint64_t seed,id;size_t n;while(scanf("%"SCNu64" %"SCNu64" %zu",&seed,&id,&n)==3){unsigned char key[16],*value=malloc(n?n:1);if(!value||n>4096)return 2;record(id,seed,key,value,n,1);for(size_t i=0;i<16;i++)printf("%02x",key[i]);putchar(' ');for(size_t i=0;i<n;i++)printf("%02x",value[i]);putchar('\n');free(value);}return ferror(stdin)?1:0;}
