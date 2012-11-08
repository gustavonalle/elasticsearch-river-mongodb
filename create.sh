#!/bin/sh
curl -XDELETE http://localhost:9200/_river/mongodb
curl -XPUT -d @create.json http://localhost:9200/_river/mongodb/_meta

