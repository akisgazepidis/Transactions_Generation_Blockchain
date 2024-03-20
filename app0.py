from kafka import KafkaConsumer
from pymongo import MongoClient
import json

# Kafka consumer for partition 0
consumer = KafkaConsumer('Blocks', bootstrap_servers='localhost:9999', group_id='group0')

# MongoDB connection
client = MongoClient('localhost', 27017)
db = client['blockchain']
blocks_collection = db['blocks']

# Read from Kafka and write to MongoDB
for message in consumer:
    block_data = json.loads(message.value.decode('utf-8'))
    if block_data['block_serial_number'] % 2 == 0:
        blocks_collection.insert_one(block_data)
