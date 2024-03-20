from pymongo import MongoClient
from bson.son import SON

# MongoDB connection
client = MongoClient('localhost', 27017)
db = client['blockchain']
blocks_collection = db['blocks']

def get_block_info(block_serial_number):
    block = blocks_collection.find_one({"block_serial_number": block_serial_number})
    if block:
        return {
            "nonce": block["nonce"],
            "digest": block["digest"],
            "num_transactions": block["num_transactions"]
        }
    else:
        return None

def smallest_mining_time():
    smallest_block = blocks_collection.find_one(sort=[("time_to_mine", 1)])
    return smallest_block

def average_and_cumulative_mining_time():
    cursor = blocks_collection.find()
    total_time = 0
    num_blocks = 0
    for block in cursor:
        total_time += block["time_to_mine"]
        num_blocks += 1
    average_time = total_time / num_blocks if num_blocks > 0 else 0
    return {
        "average_mining_time": average_time,
        "cumulative_mining_time": total_time
    }

def blocks_with_largest_num_transactions():
    largest_blocks = blocks_collection.aggregate([
        {"$group": {"_id": None, "max_transactions": {"$max": "$num_transactions"}}},
        {"$lookup": {
            "from": "blocks",
            "localField": "max_transactions",
            "foreignField": "num_transactions",
            "as": "blocks"
        }},
        {"$unwind": "$blocks"},
        {"$project": {"_id": 0, "block_serial_number": "$blocks.block_serial_number", "num_transactions": "$blocks.num_transactions"}}
    ])
    return list(largest_blocks)

def main():
    print("1. For a given block serial number, what is its nonce value, digest, and number of transactions contained in the block.")
    block_serial_number = int(input("Enter block serial number: "))
    block_info = get_block_info(block_serial_number)
    if block_info:
        print("Nonce:", block_info["nonce"])
        print("Digest:", block_info["digest"])
        print("Number of Transactions:", block_info["num_transactions"])
    else:
        print("Block not found.")

    print("\n2. Which of the blocks mined so far has the smallest mining time.")
    smallest_block = smallest_mining_time()
    print("Block Serial Number:", smallest_block["block_serial_number"])
    print("Mining Time:", smallest_block["time_to_mine"])

    print("\n3. Which is the average and cumulative mining time of all blocks mined so far.")
    mining_time_info = average_and_cumulative_mining_time()
    print("Average Mining Time:", mining_time_info["average_mining_time"])
    print("Cumulative Mining Time:", mining_time_info["cumulative_mining_time"])

    print("\n4. Which block(s) has the largest number of transactions in them.")
    largest_blocks = blocks_with_largest_num_transactions()
    for block in largest_blocks:
        print("Block Serial Number:", block["block_serial_number"])
        print("Number of Transactions:", block["num_transactions"])

if __name__ == "__main__":
    main()
