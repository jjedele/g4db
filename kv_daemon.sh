#!/usr/bin/env bash

PORT=$1
CACHE_SIZE=$2
CACHE_STRATEGY=$3
SEED_NODES=$4

nohup java -DseedNodes=$4 -jar server.jar $PORT $CACHE_SIZE $CACHE_STRATEGY 1>logs/stdout.log 2>logs/stderr.log &
echo $! > kv_$PORT.pid