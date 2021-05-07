#!/usr/bin/bash
#docker run --rm -d -it -p 4222:4222 --name=nats1 nats
docker run --rm -d -p 4222:4222 -p 8222:8222 --name=nats1 nats-streaming -store file -dir datastore
