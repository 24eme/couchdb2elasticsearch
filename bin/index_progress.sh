#!/bin/bash

cd $(dirname $0)"/.." > /dev/null

DEBUG=$1;

ls seqs/* | while read seq_file ; do
	config_file=$(grep -l $seq_file config*)
	couchdb=$(grep couchdb $config_file | sed 's/.*= *"//' | sed 's/".*//' )
	seq_couchdb=$(curl -s $couchdb | sed 's/.*update_seq":"*//' | sed 's/["}].*//' | sed 's/-.*//' | sed 's/{/10000000000/')
    if test "$seq_couchdb" -gt 0;  then
         echo -n $seq_file" : "$(echo "0"$( cat $seq_file | sed 's/-.*/ /')" * 100 / "$seq_couchdb | bc)"% "
    else
         echo -n $seq_file' : 0%'
    fi
	if test "$DEBUG"; then
  		echo -n " [ "$( cat $seq_file )" / "$seq_couchdb" ] ";
	fi
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
