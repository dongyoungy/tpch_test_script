#!/bin/bash
nodes="nodes"

let "count=0"
echo "flushing cache @ compute nodes..."
for node in $(cat ${nodes}); do
  (ssh -o "StrictHostKeyChecking no" -p 22 $node "sudo sync && echo 3 | sudo tee /proc/sys/vm/drop_caches") &
  let "count += 1"
done

wait
sleep 60
echo "DONE"
