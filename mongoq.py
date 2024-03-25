from pymongo import MongoClient

# MongoDB configuration
client = MongoClient('localhost', 27017)
db = client['mydatabase']
collection = db['blocks']


def get_block_info(serial_number):
    block = collection.find_one({'serial_number': serial_number})
    if block:
        block_info = {}
        if 'nonce' in block:
            block_info['nonce'] = block['nonce']
        if 'hash' in block:
            block_info['hash'] = block['hash']
        if 'transactions' in block:
            block_info['num_transactions'] = len(block['transactions'])
        return block_info
    else:
        return None


def get_block_with_smallest_mining_time():
    block = collection.find_one({}, sort=[('mining_time', 1)])
    return block


def get_average_and_cumulative_mining_time():
    total_mining_time = 0
    num_blocks = 0
    for block in collection.find():
        if 'mining_time' in block:
            total_mining_time += block['mining_time']
            num_blocks += 1
    if num_blocks > 0:
        average_mining_time = total_mining_time / num_blocks
    else:
        average_mining_time = 0
    return {
        'average_mining_time': average_mining_time,
        'cumulative_mining_time': total_mining_time
    }


def get_blocks_with_largest_num_transactions():
    max_transactions = collection.aggregate([
        {'$project': {'num_transactions': {'$size': '$transactions'}}},
        {'$group': {'_id': None, 'max_transactions': {'$max': '$num_transactions'}}}
    ])
    max_transactions_count = max_transactions.next()['max_transactions']

    blocks = collection.find({'transactions': {'$size': max_transactions_count}})
    return list(blocks)


if __name__ == "__main__":
    # Test the functions
    serial_number = int(input("Enter the block serial number: "))
    block_info = get_block_info(serial_number)
    if block_info:
        print(f"Block info for serial number {serial_number}:")
        print(block_info)
    else:
        print("Block not found.")

    print("\n2. Block with the smallest mining time:")
    print(get_block_with_smallest_mining_time())

    print("\n3. Average and cumulative mining time of all blocks:")
    print(get_average_and_cumulative_mining_time())

    print("\n4. Block(s) with the largest number of transactions:")
    blocks_with_largest_num_transactions = get_blocks_with_largest_num_transactions()
    for block in blocks_with_largest_num_transactions:
        print(block)
