#!/usr/bin/env python3
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
import audit

class IndependentHistoryTests(unittest.TestCase):
    def sample(self):
        c=dict(key_mode='id',payload_mode='seeded',record_seed=0,payload_bytes=8,count=3,
               topic='t',partitions=1,acks=-1,idempotence=True,isolation_level='read_committed')
        raw=dict(timed=dict(acknowledged=3),high_watermarks=dict(partitions=[dict(partition=0,start_offset=7,end_offset=10)]))
        attempts=[]; receipts=[]
        for index in range(3):
            key,value=audit.record(0,index,8)
            attempts.append(dict(id=str(index),attempt_index=index,topic='t',partition=0,key=key.hex(),
                                 payload_hash=hashlib.sha256(value).hexdigest(),status='acked',offset=7+index))
            receipts.append(dict(partition=0,offset=7+index,key=key.hex(),value=value.hex()))
        return c,raw,attempts,receipts

    def test_published_generator_vectors(self):
        key,value=audit.record(0,0,8)
        self.assertEqual(key.hex(),'0000000000000000e220a8397b1dcdaf')
        self.assertEqual(value.hex(),'e220a8397b1dcdaf')

    def test_valid_independent_receipts(self):
        _,verdict=audit.verify(*self.sample())
        self.assertTrue(verdict['valid'],verdict)

    def test_missing_duplicate_swap_is_detected(self):
        c,r,a,receipts=self.sample(); receipts[2]=receipts[1].copy()
        self.assertFalse(audit.verify(c,r,a,receipts)[1]['valid'])

    def test_payload_corruption_is_detected_independently(self):
        c,r,a,receipts=self.sample(); receipts[1]['value']='00'*8
        self.assertFalse(audit.verify(c,r,a,receipts)[1]['valid'])

    def test_producer_and_receipt_same_corruption_still_fails(self):
        c,r,a,receipts=self.sample(); receipts[0]['value']='00'*8
        a[0]['payload_hash']=hashlib.sha256(bytes(8)).hexdigest()
        self.assertFalse(audit.verify(c,r,a,receipts)[1]['valid'])

    def test_wrong_key_distinguisher_fails(self):
        c,r,a,receipts=self.sample(); receipts[0]['key']='00'*16
        self.assertFalse(audit.verify(c,r,a,receipts)[1]['valid'])

    def test_wrong_partition_or_offset_fails(self):
        for field,value in [('partition',1),('offset',100)]:
            c,r,a,receipts=self.sample(); receipts[1][field]=value
            self.assertFalse(audit.verify(c,r,a,receipts)[1]['valid'])

    def test_partition_order_inversion_fails(self):
        c,r,a,receipts=self.sample(); receipts[0]['offset'],receipts[1]['offset']=8,7
        receipts[0],receipts[1]=receipts[1],receipts[0]
        self.assertFalse(audit.verify(c,r,a,receipts)[1]['valid'])

    def test_acks_zero_cannot_be_marked_acknowledged(self):
        c,r,a,receipts=self.sample(); c['acks']=0; c['idempotence']=False
        self.assertFalse(audit.verify(c,r,a,receipts)[1]['valid'])

    def test_null_key_history_is_explicitly_unsupported(self):
        c,r,a,receipts=self.sample(); c['key_mode']='none'
        with self.assertRaisesRegex(ValueError,'unique ID'):
            audit.verify(c,r,a,receipts)

    def test_empty_receipts_do_not_pass(self):
        c,r,a,_=self.sample()
        self.assertFalse(audit.verify(c,r,a,[])[1]['valid'])

if __name__=='__main__': unittest.main()
