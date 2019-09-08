#!/bin/bash
# Docker build, tag and push

docker build ./kafka_consumer/ -t vitekjede/franz-ivan-consumer
docker push vitekjede/franz-ivan-consumer
docker build ./kafka_producer/ -t vitekjede/franz-ivan-producer
docker push vitekjede/franz-ivan-producer