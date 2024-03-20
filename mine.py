from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from hashlib import sha256
import json
import time

# Genesis block
genesis_block = {
    "prev_hash": "",
    "transactions": [],
    "nonce": 0,
    "timestamp": time.time()
}
blockchain = [genesis_block]
block_serial_number = 0

# Function to mine a block
def mine_block(prev_hash, transactions, difficulty):
    block = {
        "prev_hash": prev_hash,
        "transactions": transactions,
        "nonce": 0,
        "timestamp": time.time()
    }
    start_time = time.time()
    while not is_valid_block(block, difficulty):
        block["nonce"] += 1
    end_time = time.time()
    return block, end_time - start_time

# Function to check block validity based on difficulty
def is_valid_block(block, difficulty):
    block_hash = calculate_hash(block)
    return block_hash.startswith('0' * difficulty)

# Function to calculate the hash of a block
def calculate_hash(block):
    block_string = str(block["prev_hash"]) + str(block["transactions"]) + str(block["nonce"]) + str(block["timestamp"])
    return sha256(block_string.encode()).hexdigest()

# Function to print the blockchain
def print_blockchain():
    for block in blockchain:
        print(block)

# Function to write block information to Kafka
def write_to_kafka(block, time_to_mine):
    global block_serial_number
    block_serial_number += 1
    kafka_producer.send('Blocks', json.dumps({
        "block_serial_number": block_serial_number,
        "num_transactions": len(block["transactions"]),
        "nonce": block["nonce"],
        "digest": calculate_hash(block),
        "time_to_mine": time_to_mine
    }), partition=block_serial_number % 2)

# Main function
def main():
    # Initialize SparkContext and StreamingContext
    sc = SparkContext(appName="BlockchainMining")
    ssc = StreamingContext(sc, 120)  # 2-minute interval

    # Kafka parameters
    kafka_params = {"metadata.broker.list": "localhost:9092"}

    # Create a DStream that connects to the server and receives transactions
    transactions_stream = KafkaUtils.createStream(ssc, "localhost:2181", "spark-streaming-consumer", {"transactions": 1})

    # Process transactions and mine blocks
    def process_transactions(time, rdd):
        transactions = rdd.collect()
        last_block = blockchain[-1]
        mined_block, time_to_mine = mine_block(last_block["prev_hash"], transactions, sc.defaultParallelism)
        blockchain.append(mined_block)
        print("New block mined:")
        print_blockchain()
        write_to_kafka(mined_block, time_to_mine)

    transactions_stream.foreachRDD(process_transactions)

    # Start the streaming context
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    from kafka import KafkaProducer
    kafka_producer = KafkaProducer(bootstrap_servers='localhost:9999', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    main()

