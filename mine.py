from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaProducer
from datetime import datetime
import json
import hashlib
import time
import random
import string

# Global variable to store the blockchain
blockchain = []

# Define a pool of possible characters for transaction data
CHARACTER_POOL = string.ascii_letters + string.digits + string.punctuation


def generate_random_transaction():
    """
    Generates a random transaction data.
    """
    transaction_length = random.randint(10, 50)  # Random length for each transaction
    transaction_data = ''.join(random.choices(CHARACTER_POOL, k=transaction_length))
    return transaction_data


def mine_block(transactions, producer):
    global blockchain

    # Start timing the mining process
    start_time = time.perf_counter()

    if not blockchain:  # If blockchain is empty, add the genesis block
        genesis_block = create_genesis_block()
        blockchain.append(genesis_block)
        previous_hash = genesis_block['hash']
    else:
        previous_hash = blockchain[-1]['hash']

    # Generate random transactions
    num_transactions = random.randint(1, 5)  # Random number of transactions per block
    transactions = [generate_random_transaction() for _ in range(num_transactions)]

    block = create_block(len(blockchain), transactions, previous_hash)

    # Mine the block
    block = mine_block_hash(block)

    # Add the mined block to the blockchain
    blockchain.append(block)

    # End timing the mining process
    end_time = time.perf_counter()
    mining_duration = round((end_time - start_time), 5)

    # Print block details and blockchain
    print_block_details(block, mining_duration)
    print_blockchain()

    # Sending block to Kafka
    send_block_to_kafka(block, producer)


def create_genesis_block():
    genesis_block = {
        'serial_number': 0,
        'timestamp': datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
        'transactions': 'Genesis block',
        'previous_hash': '0',  # Hash of previous block is '0'
        'nonce': 0
    }
    # Calculate hash for genesis block
    genesis_block_string = json.dumps(genesis_block, sort_keys=True)
    genesis_block_hash = hashlib.sha256(genesis_block_string.encode()).hexdigest()
    genesis_block['hash'] = genesis_block_hash
    return genesis_block


def create_block(serial_number, transactions, previous_hash):
    return {
        'serial_number': serial_number,
        'timestamp': datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
        'transactions': transactions,
        'previous_hash': previous_hash,
        'nonce': 1  # Initialize nonce as an integer
    }


def mine_block_hash(block):
    prefix = '000'  # Target hash prefix
    while True:
        block_string = json.dumps(block, sort_keys=True)
        block_hash = hashlib.sha256(block_string.encode()).hexdigest()
        if block_hash.startswith(prefix):
            block['hash'] = block_hash
            break  # Correct nonce found
        else:
            block['nonce'] += 1  # Increment nonce
            block['nonce'] %= 2**32  # Ensure nonce wraps around at 32-bit limit
    return block


def print_block_details(block, mining_duration):
    print(f"Block Serial Number: {block['serial_number']}")
    print(f"Number of Transactions: {len(block['transactions'])}")
    print(f"Block Nonce: {block['nonce']}")
    print(f"Block’s Digest (Hash): {block['hash']}")
    print(f"Time to Mine: {mining_duration} seconds")


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


def send_block_to_kafka(block, producer):
    partition = 0 if block['serial_number'] % 2 == 0 else 1
    producer.send("Blocks", key=b'even' if partition == 0 else b'odd', value=json.dumps(block).encode('utf-8'))


def main():
    # Create Kafka topic with two partitions
    create_kafka_topic()

    # Initialize Kafka producer
    kafka_producer = create_kafka_producer()

    while True:
        mine_block([], kafka_producer)


if __name__ == "__main__":
    main()
