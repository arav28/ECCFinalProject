from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
import csv
import json

def produce_messages(file_path, topic):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
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

def create_airflow_dag():
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 4, 25),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    dag = DAG(
        'kafka_producer_dag',
        default_args=default_args,
        description='A DAG to produce messages to Kafka topic',
        schedule=timedelta(days=1),
    )

    produce_messages_task = PythonOperator(
        task_id='produce_messages',
        python_callable=produce_messages,
        op_args=['sample_data.csv', 'raw_ecommerce_data'],
        dag=dag,
    )

    return dag

if __name__ == "__main__":
    dag = create_airflow_dag()

