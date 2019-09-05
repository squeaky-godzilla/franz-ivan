from kafka import KafkaConsumer, TopicPartition, SimpleConsumer, SimpleClient
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

consumer = KafkaConsumer(bootstrap_servers=":".join([KAFKA_HOST, KAFKA_PORT]), auto_offset_reset='earliest')

consumer.assign([TopicPartition(KAFKA_TOPIC, 0)])

for message in consumer:
    json_string = message.value.decode('utf8')
    try:
        json_data = json.loads(json_string)
        print(json_data)
    except:
        pass