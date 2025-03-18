#!/bin/bash

cd $(dirname $0)/..

find config*php | while read -r config ; do
    curl -X PUT -d @/home/actualys/couchdb2elasticsearch/giilda_mapping.json "$( grep elastic_url_db "$config" | sed 's/'\''/"/g' | awk -F '"' '{print $2}' )"
done
