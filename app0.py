from kafka import KafkaConsumer
from pymongo import MongoClient
import json


def connect_to_kafka_consumer(topic, bootstrap_servers, group_id, auto_offset_reset='earliest'):
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: x.decode('utf-8')
    )


def connect_to_mongodb(database_name, collection_name, host='localhost', port=27017):
    client = MongoClient(host, port)
    db = client[database_name]
    return db[collection_name]


def consume_messages_and_write_to_mongodb(consumer, collection):
    for message in consumer:
        block_data = json.loads(message.value)
        if block_data.get('serial_number', 0) % 2 == 0:  # Ensure the block belongs to partition 0
            collection.insert_one(block_data)
            print(f"Block written to MongoDB: {block_data}")


def main():
    kafka_topic = 'Blocks'
    kafka_bootstrap_servers = ['localhost:9092']
    kafka_group_id = 'block-consumer-group-0'

    mongodb_database_name = 'mydatabase'
    mongodb_collection_name = 'blocks'

    consumer = connect_to_kafka_consumer(kafka_topic, kafka_bootstrap_servers, kafka_group_id)
    collection = connect_to_mongodb(mongodb_database_name, mongodb_collection_name)

    consume_messages_and_write_to_mongodb(consumer, collection)


if __name__ == "__main__":
    main()
