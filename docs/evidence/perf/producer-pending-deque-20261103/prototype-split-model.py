#!/usr/bin/env python3
"""Exhaustive *model* of Split's logical permutations; no Rust code executes."""
from itertools import product

def restore(records, dest, inverse):
    records=records[:]; dest=dest[:]
    for pos in range(len(records)):
        while True:
            old=inverse[dest[pos]]
            if old==pos: break
            records[pos],records[old]=records[old],records[pos]
            dest[pos],dest[old]=dest[old],dest[pos]
    return records

cases=0; checkpoints=0
for n in range(1,9):
 for start in range(n+1):
  for mask in product((False,True),repeat=n-start):
   selected=[i for i in range(start,n) if mask[i-start]]
   kept=[i for i in range(n) if i not in selected]
   if selected and len(selected)!=n and selected[0]!=len(kept):
    inverse=kept+selected
    dest=[inverse.index(i) for i in range(n)]
    records=list(range(n))
    for pos in range(n):
     while dest[pos]!=pos:
      target=dest[pos]
      records[pos],records[target]=records[target],records[pos]
      dest[pos],dest[target]=dest[target],dest[pos]
      assert restore(records,dest,inverse)==list(range(n)),(n,start,mask,pos)
      checkpoints+=1
    assert records==inverse
    prefix=records[:len(kept)]; suffix=records[len(kept):]
    assert prefix==kept and suffix==selected
    assert restore(prefix+suffix,dest,inverse)==list(range(n))
   else:
    assert kept+selected == ([i for i in range(n) if i not in selected]+selected)
   cases+=1
print(f'{cases} modeled cases, {checkpoints} modeled partial-permutation rollback checkpoints passed; no actual deque, Tokio, allocator or Rust execution')
