## Tools to Install:
    - Install Docker (from the original website)
    - Install Java (from the original website) + fix the home paths of java
    - Install MongoDB - In the environment’s terminal in PyCharm run the following: docker pull mongodb/mongodb-community-server:6.0-ubuntu2204
    - Install Kafka - In the environment’s terminal in PyCharm run the following: docker pull apache/kafka:3.7.0

With docker we will pull the images we need in order to use Kafka and MongoDB for the project.

Start the procedure:
    - In the environment’s terminal in PyCharm run the following for mongoDB: docker run --name mongodb -d -p 27017:27017 mongodb/mongodb-community-server:6.0-ubuntu2204
    - In the environment’s terminal in PyCharm run the following for Kafka: docker run --name kafka -p 9092:9092 apache/kafka:3.7.0

Run the previous commands in terminal in order to start the containers in docker.

Run the .py files in command line:
    - tr-server.py
    - mine.py
    - app0.py
    - app1.py
    - mongoq.py

It is essential to run the previous py files in differnent command lines and in the same order so as to see the transactions created and handled,
also to query a specific number in the mongoq.py file and check the results.