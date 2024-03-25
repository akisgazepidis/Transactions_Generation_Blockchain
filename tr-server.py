import socket
import time



def start():
    # Create a TCP/IP socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Bind the socket to the port
    server_address = ('localhost', 9999)
    print('Starting up on {} port {}'.format(*server_address))
    server_socket.bind(server_address)

    # Listen for incoming connections
    server_socket.listen(1)

    try:
        while True:
            print('Waiting for a connection...')
            connection, client_address = server_socket.accept()
            print('Connection from', client_address)

            try:
                counter = 0
                while True:
                    # Generate a transaction (an arbitrary string)
                    transaction = f"Sample transaction_{counter}"
                    print(transaction)
                    counter += 1

                    # Send the transaction
                    connection.sendall((transaction + "\n").encode())



                    # Sleep for one second
                    time.sleep(1)
            finally:
                # Clean up the connection
                connection.close()
    finally:
        # Clean up the server socket
        server_socket.close()


if __name__ == "__main__":
    start()
