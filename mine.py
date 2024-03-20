from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from hashlib import sha256
import time

# Genesis block
genesis_block = {
    "prev_hash": "",
    "transactions": [],
    "nonce": 0,
    "timestamp": time.time()
}
blockchain = [genesis_block]

# Function to mine a block
def mine_block(prev_hash, transactions, difficulty):
    block = {
        "prev_hash": prev_hash,
        "transactions": transactions,
        "nonce": 0,
        "timestamp": time.time()
    }
    while not is_valid_block(block, difficulty):
        block["nonce"] += 1
    return block

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

# Main function
def main():
    # Initialize SparkContext and StreamingContext
    sc = SparkContext(appName="BlockchainMining")
    ssc = StreamingContext(sc, 120)  # 2-minute interval

    # Create a DStream that connects to the server and receives transactions
    transactions_stream = ssc.socketTextStream("localhost", 9999)

    # Process transactions and mine blocks
    def process_transactions(time, rdd):
        transactions = rdd.collect()
        last_block = blockchain[-1]
        mined_block = mine_block(last_block["prev_hash"], transactions, sc.defaultParallelism)
        blockchain.append(mined_block)
        print("New block mined:")
        print_blockchain()

    transactions_stream.foreachRDD(process_transactions)

    # Start the streaming context
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()
