from kafka import KafkaConsumer, TopicPartition, SimpleConsumer, SimpleClient
import json
import argparse

# Postgres imports

from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import OperationalError
from psycopg2 import errors


parser = argparse.ArgumentParser()

parser.add_argument('--topic',action='store',dest='kafka_topic', required=True)
parser.add_argument('--host',action='store',dest='kafka_host', required=True)
parser.add_argument('--port',action='store',dest='kafka_port', required=True)


parser.add_argument('--db-host',action='store',dest='db_host', required=True)
# parser.add_argument('--db-port',action='store',dest='db_port', required=True)


args = parser.parse_args()

KAFKA_TOPIC = args.kafka_topic
KAFKA_HOST = args.kafka_host
KAFKA_PORT = str(args.kafka_port)
DB_HOST = args.db_host

DB_NAME = "crypto_prices"

consumer = KafkaConsumer(bootstrap_servers=":".join([KAFKA_HOST, KAFKA_PORT]), auto_offset_reset='latest')

consumer.assign([TopicPartition(KAFKA_TOPIC, 0)])

# set up Postgres connection and prices database
def init_connect_dbase():
    try:
        db_connection = connect(user='postgres',password='example',host=DB_HOST, database=DB_NAME)
        db_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    except OperationalError:
        print("first time record, creating DB...")
        db_connection = connect(user='postgres',password='example',host=DB_HOST)
        db_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        db_cursor = db_connection.cursor()
        db_cursor.execute("CREATE DATABASE %s" % DB_NAME)
        # close cursor and connection and reconnect
        db_cursor.close()
        db_connection.close()
        db_connection = connect(user='postgres',password='example',host=DB_HOST, database=DB_NAME)
    return db_connection

db_connection = init_connect_dbase()

# set up cursor
db_cursor = db_connection.cursor()

# create table to record the time series
try:
    db_cursor.execute('CREATE TABLE %s (timestamp bigint PRIMARY KEY, value float)' % KAFKA_TOPIC)
except errors.DuplicateTable:
    print('recording to existing table: %s' % KAFKA_TOPIC)

# create table to record Cryptowat.ch API call allowance
try:
    db_cursor.execute('CREATE TABLE cryptowatch_api_allowance (timestamp bigint PRIMARY KEY, allowance bigint)')
except errors.DuplicateTable:
    pass

    

for message in consumer:
    json_string = message.value.decode('utf8')
    
    try:
        json_data = json.loads(json_string)
        
        # record time series to Postgres
        db_cursor.execute('INSERT INTO %s(timestamp, value) VALUES (%s, %s)' % 
            (KAFKA_TOPIC, str(message.timestamp), str(json_data['result']['price'])))
        
        # record API call allowance to Postgres
        db_cursor.execute('INSERT INTO %s(timestamp, allowance) VALUES (%s, %s)' % 
            ("cryptowatch_api_allowance", str(message.timestamp), str(json_data['allowance']['remaining'])))
        print(json_data)
    except:
        pass