from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import hashlib
import time
from datetime import datetime
from kafka import KafkaProducer

# Global variable to store the blockchain
blockchain = []

def mine_block(transactions, producer):
    global blockchain

    # Start timing the mining process
    start_time = time.perf_counter()

    if not blockchain:  # If blockchain is empty, add the genesis block
        genesis_block = {
            'serial_number': 0,
            'timestamp': datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
            'transactions': 'Genesis block',
            'previous_hash': '0',  # Hash of previous block is '0'
            'nonce': 0
        }
        blockchain.append(genesis_block)
        previous_hash = '0'
    else:
        previous_hash = blockchain[-1]['hash']

    block = {
        'serial_number': len(blockchain),
        'timestamp': datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
        'transactions': transactions,
        'previous_hash': previous_hash,
        'nonce': 1,  # Initialize nonce as an integer
    }

    # Prepare for the proof of work algorithm
    prefix = '000'  # Target hash prefix
    while True:
        block_string = json.dumps(block, sort_keys=True)
        block_hash = hashlib.sha256(block_string.encode()).hexdigest()
        if block_hash.startswith(prefix):
            break  # Correct nonce found
        else:
            block['nonce'] += 1  # Increment nonce
            block['nonce'] %= 2**32  # Ensure nonce wraps around at 32-bit limit

    # Set the block's hash
    block['hash'] = block_hash

    # Add the mined block to the blockchain
    blockchain.append(block)

    # End timing the mining process
    end_time = time.perf_counter()
    mining_duration = round((end_time - start_time), 5)

    # Printing the requested details
    print(f"Block Serial Number: {block['serial_number']}")
    print(f"Number of Transactions: {len(block['transactions'])}")
    print(f"Block Nonce: {block['nonce']}")
    print(f"Block’s Digest (Hash): {block['hash']}")
    print(f"Time to Mine: {mining_duration} seconds")
    print_blockchain()

    # Sending block to Kafka
    if block['serial_number'] % 2 == 0:
        # Send block to partition 0 (even serial numbers)
        producer.send("Blocks", key=b'even', value=json.dumps(block).encode('utf-8'))
    else:
        # Send block to partition 1 (odd serial numbers)
        producer.send("Blocks", key=b'odd', value=json.dumps(block).encode('utf-8'))

def print_blockchain():
    global blockchain
    print("Blockchain:")
    for block in blockchain:
        print(block)
    print()

def create_kafka_producer():
    return KafkaProducer(bootstrap_servers='localhost:9092')

def create_kafka_topic():
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')
    topic = NewTopic(name="Blocks", num_partitions=2, replication_factor=1)
    try:
        admin_client.create_topics([topic])
    except TopicAlreadyExistsError:
        print("Topic 'Blocks' already exists.")

def main():
    # Create Kafka topic with two partitions
    create_kafka_topic()

    sc = SparkContext(appName="BlockchainMining")

    # Get the number of cores available
    num_cores = sc.defaultParallelism

    ssc = StreamingContext(sc, 120)  # 2-minute interval

    # Create a DStream that connects to the server
    dstream = ssc.socketTextStream("localhost", 9999)

    # Initialize Kafka producer
    kafka_producer = create_kafka_producer()

    # Process transactions and mine blocks
    dstream.foreachRDD(lambda rdd: mine_block(rdd.collect(), kafka_producer))

    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate

if __name__ == "__main__":
    main()
