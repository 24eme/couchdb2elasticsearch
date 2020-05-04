#!/bin/bash

cd $(dirname $0)"/.." > /dev/null

ls seqs/* | while read seq_file ; do
	config_file=$(grep -l $seq_file config*)
	couchdb=$(grep couchdb $config_file | sed 's/.*= *"//' | sed 's/".*//' )
	seq_couchdb=$(curl -s $couchdb | sed 's/.*update_seq":"*//' | sed 's/["}].*//' | sed 's/-.*//' | sed 's/{/10000000000/')
	echo -n $seq_file" : "$(echo "*"$( cat $seq_file | sed 's/-.*//' )"0 * 100 / "$seq_couchdb | bc)"% "
	if test -f "/tmp/couchdb2elasticsearch_"$config_file".lock" ; then
		if ps aux | grep $config_file | grep -v "sh " | grep -v grep > /dev/null ; then
			echo "(running)";
		else
			echo "(not running but locked)";
		fi
	else
		echo "(not running)";
	fi
done
