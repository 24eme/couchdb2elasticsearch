#!/bin/bash

ps aux | grep couchdb2elasticsearch.php | grep -v sh  | grep -v grep | awk '{print $5}' | while read pid ; do
	kill $pid;
done

ls /tmp/couchdb2elasticsearch*
