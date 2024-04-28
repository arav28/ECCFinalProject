**CONSUMER**

These files contain Python code for consuming data from a Kafka topic, processing it with Spark, and generating recommendations.

1. Functionality

    consumer.py and consumer2.py perform similar functions, but differ in the recommendation algorithm used.
    Both programs:
        Consume JSON messages from a Kafka topic named "raw_ecommerce_data".
        Process the data using Spark SQL, including filtering and adding a "GroupPrice" column.
        Train a model using the processed data.
        Generate recommendations based on the model.
    consumer.py trains an FP-Growth model for market basket analysis and recommends frequently bought together items.
    consumer2.py trains an ALS model for collaborative filtering and recommends items similar to those a customer has previously purchased.

2. Requirements

    Python 3.x
    Apache Kafka
    PySpark
    json library

3. Usage

    Ensure you have a Kafka cluster running with a topic named "raw_ecommerce_data".

    Install the required libraries:
    Bash

    pip install kafka-python pyspark json

    Use code with caution.

Run the desired script:

    For FP-Growth recommendations:
    Bash

    python consumer.py

    Use code with caution.

For collaborative filtering recommendations:
Bash

python consumer2.py

Use code with caution.

4. Notes

    The code assumes a basic understanding of Kafka, Spark, and recommendation algorithms.
    You may need to modify the connection details (e.g., Kafka bootstrap servers) based on your environment.
    The example customer ID (customer_id) in consumer2.py needs to be replaced with a real customer ID.

5. Choosing the Right Script

    Use consumer.py if you want to identify frequently bought together items.
    Use consumer2.py if you want to recommend items similar to a customer's past purchases.


**PRODUCER**
