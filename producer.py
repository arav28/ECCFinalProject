from kafka import KafkaProducer
import csv
import json

def produce_messages(file_path, producer, topic):
    messages = []  # List to store messages
    with open(file_path, 'r', newline='', encoding='latin-1') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            # Convert each row to JSON format and append to the list
            message = json.dumps(row)
            messages.append(message)
            print(f"Produced message: {message}")  # Print after adding each message

    # Send all messages in a single batch
    for message in messages:
        producer.send(topic, message.encode('utf-8'))

if __name__ == "__main__":
    bootstrap_servers = 'localhost:9092'
    input_topic = 'raw_ecommerce_data'
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    file_path = 'sample_data.csv'
    produce_messages(file_path, producer, input_topic)
