#!/usr/bin/env bash

USER_NAME="jeffrey.jedele"
SERVER_NODES="db-1 db-2 db-3 db-4 db-5"
CLIENT_NODES="client-1 client-2 client-3"

for node in $SERVER_NODES $CLIENT_NODES
do
    ssh $USER_NAME@$node -x "sudo apt-get update && sudo apt-get -y install openjdk-11-jre tmux"
    scp ms3-{server,client}.jar $USER_NAME@$node:~
done
