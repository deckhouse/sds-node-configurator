#!/bin/sh

thinPoolState=`nsenter lsmod | grep -E '^dm_thin_pool' | wc -l`
echo "Checking dm_thin_pool module presence"
if [ $thinPoolState -ne 1 ]
then
  echo "Loading dm_thin_pool"
  nsenter modprobe dm_thin_pool
fi
