#!/bin/bash

cd $(dirname $0)"/.." > /dev/null

ls seqs/* | while read seq_file ; do
	couchdb=$(grep couchdb $(grep -l $seq_file config*) | sed 's/.*= "//' | sed 's/".*//' )
	seq_couchdb=$(curl -s $couchdb | sed 's/.*update_seq"://' | sed 's/["}].*//')
	echo $seq_file" : "$(echo $( cat $seq_file )" * 100 / "$seq_couchdb | bc)%
done
