from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array_contains, collect_set
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.recommendation import ALS
import json
from functools import reduce

def process_data(spark, data):
  # Convert data to Spark DataFrame
  data_df = spark.createDataFrame([data])
  # Perform any transformations or analysis using Spark
  data_df = data_df.dropna()

  # Add a column for total price
  data_df = data_df.withColumn('GroupPrice', col('Quantity') * col('UnitPrice'))

  return data_df

def consume_from_kafka_and_process(timeout_ms=3000):
  consumer = KafkaConsumer('raw_ecommerce_data', bootstrap_servers=['localhost:9092'],
                           consumer_timeout_ms=timeout_ms)

  spark = SparkSession.builder \
      .appName("KafkaConsumer") \
      .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  all_data = []

  for message in consumer:
    data = json.loads(message.value.decode('utf-8'))
    print("Received message:", data)
    all_data.append(data)

  consumer.close()
  print("ALLLLDATA", all_data)

  if all_data:
    # Process all data
    processed_data = [process_data(spark, data) for data in all_data]
    all_processed_data = reduce(lambda df1, df2: df1.union(df2), processed_data)

    # FP-Growth Model

    basket = all_processed_data.groupBy('InvoiceNo', 'CustomerID') \
        .agg(collect_set('StockCode').alias('StockCodeList'))

    fp_growth = FPGrowth(itemsCol='StockCodeList', minSupport=0.025, minConfidence=0.3)
    model = fp_growth.fit(basket)

    # Get association rules
    association = model.associationRules
    print('Number of association rules:', association.count())
    print(association.show(10))

    # Collaborative Filtering using ALS
    als = ALS(rank=50, maxIter=10, regParam=0.1, userCol="CustomerID", itemCol="StockCode")
    model = als.fit(all_processed_data)

    # Generate recommendations for a specific customer
    customer_id = 123  # Replace with actual customer ID
    user_df = all_processed_data.filter(col("CustomerID") == customer_id)
    recommendations = model.recommendForUser(user_df, 10)

    print("Top Recommendations for Customer", customer_id)
    recommendations.show()

  spark.stop()

if __name__ == "_main_":
  consume_from_kafka_and_process()
