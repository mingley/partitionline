"""Replay actual open-loop samples and their terminal accounting.

Derived from the retained KL09-12 latency controller; no synthetic timings.
"""
def validate_open_loop(samples, summary, rate):
    count=20000
    if len(samples)!=count or summary['kind']!='open_loop_produce_ack': raise ValueError('raw population differs')
    if summary['rate_per_second']!=rate or summary['warmup_records']!=10000: raise ValueError('rate/warmup differs')
    outcomes=summary['outcomes']; ack=outcomes['acknowledged']; rejected=outcomes['rejected']
    if outcomes != dict(offered=count,accepted=ack,acknowledged=ack,consumed=0,rejected=rejected,timed_out=0,unknown=0):
        raise ValueError('terminal accounting differs or delivery is ambiguous')
    if ack+rejected!=count or rejected!=summary['capacity_rejections']: raise ValueError('capacity accounting differs')
    if summary['run_disposition']!=('failed' if rejected else 'executed'): raise ValueError('failed disposition lost')
    if not summary['coordinated_omission_avoidance'] or not summary['warmup_excluded']: raise ValueError('schedule semantics differ')
    distributions={k:[] for k in ('end_to_end','enqueue_to_ack_upper_bound','schedule_lag','enqueue_wait_upper_bound')}
    actual_ack=0
    for i,sample in enumerate(samples):
        intended=sample['intended_arrival_ns'];offered=sample['actual_offer_ns'];completed=sample['completed_ns']
        if sample['id']!=i or intended!=i*1_000_000_000//rate: raise ValueError('absolute arrival schedule differs')
        if not all(isinstance(v,int) and v>=0 for v in (intended,offered,completed)) or not intended<=offered<=completed:
            raise ValueError('invalid raw schedule timing')
        lag=offered-intended
        if sample['schedule_lag_ns']!=lag: raise ValueError('schedule lag differs')
        distributions['schedule_lag'].append(lag//1000)
        if sample['outcome']=='rejected':
            if sample['accepted'] or sample['error']!='benchmark pending capacity exhausted': raise ValueError('rejection differs')
            if any(sample[k] is not None for k in ('actual_enqueue_lower_ns','actual_enqueue_upper_ns',
                    'acknowledgment_observed_ns','end_to_end_ns','enqueue_to_ack_upper_ns','enqueue_wait_upper_ns')):
                raise ValueError('rejected record fabricated acceptance or acknowledgment')
            continue
        if sample['outcome']!='acknowledged' or not sample['accepted'] or sample['error'] is not None:
            raise ValueError('terminal record differs')
        actual_ack+=1
        lower,upper,seen=[sample[k] for k in ('actual_enqueue_lower_ns','actual_enqueue_upper_ns','acknowledgment_observed_ns')]
        if not all(isinstance(v,int) and v>=0 for v in (lower,upper,seen)) or not offered<=lower<=upper<=seen==completed:
            raise ValueError('causal acknowledgment timing differs')
        for key,field,value in (('end_to_end','end_to_end_ns',seen-intended),
                ('enqueue_to_ack_upper_bound','enqueue_to_ack_upper_ns',seen-lower),
                ('enqueue_wait_upper_bound','enqueue_wait_upper_ns',upper-offered)):
            if sample[field]!=value: raise ValueError('derived duration differs')
            distributions[key].append(value//1000)
    if actual_ack!=ack: raise ValueError('raw acknowledgment count differs')
    for key,values in distributions.items():
        values.sort(); actual=summary[key]; n=len(values); eligible=n>=10000
        if actual['sample_count']!=n or actual['sample_floor_met']!=eligible: raise ValueError('sample population differs')
        for name,permille in (('p50_us',500),('p95_us',950),('p99_us',990),('p99_9_us',999)):
            expected=values[(n*permille+999)//1000-1] if eligible else None
            if actual[name]!=expected: raise ValueError('raw percentile differs')

