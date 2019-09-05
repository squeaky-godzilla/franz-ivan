from kafka import KafkaProducer
import time
import json
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--topic',action='store',dest='kafka_topic')
parser.add_argument('--host',action='store',dest='kafka_host')
parser.add_argument('--port',action='store',dest='kafka_port')

args = parser.parse_args()

KAFKA_TOPIC = args.kafka_topic
KAFKA_HOST = args.kafka_host
KAFKA_PORT = str(args.kafka_port)

# producer = KafkaProducer(value_serializer=lambda v:json.dumps(v).encode('utf-8'), bootstrap_servers=":".join([KAFKA_HOST, KAFKA_PORT]))
producer = KafkaProducer(bootstrap_servers=":".join([KAFKA_HOST, KAFKA_PORT]))

# for _ in range(100):
#     producer.send(KAFKA_TOPIC, b'some_message_bytes_with sleep')
#     time.sleep(10)

while True:
    record = b'{"result":{"price":10540.97},"allowance":{"cost":670396,"remaining":7950169726}}'
    # record = '{"result":{"price":10540.97},"allowance":{"cost":670396,"remaining":7950169726}}'
    producer.send(KAFKA_TOPIC, record)
    time.sleep(10)