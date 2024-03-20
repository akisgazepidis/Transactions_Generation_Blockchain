import socket
import time
import random
import string

PORT = 9999

def generate_transaction():
    """Generate a random unique transaction."""
    transaction = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
    return transaction

def main():
    """Main function to run the server."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('', PORT))
    server_socket.listen()

    print("Server ready: listening to port {0} for connections.\n".format(PORT))

    while True:
        client_socket, client_address = server_socket.accept()
        print("Client connected:", client_address)

        try:
            while True:
                transaction = generate_transaction()
                client_socket.send((transaction + '\n').encode())
                print("Sent transaction:", transaction)
                time.sleep(1)
        except Exception as e:
            print("An error occurred:", e)
        finally:
            client_socket.close()

if __name__ == "__main__":
    main()
