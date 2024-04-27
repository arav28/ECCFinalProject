from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array_contains, collect_set
from pyspark.ml.fpm import FPGrowth
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
    print("ALLLLDATA",all_data)

    if all_data:
        # Process all data
        processed_data = [process_data(spark, data) for data in all_data]
        all_processed_data = reduce(lambda df1, df2: df1.union(df2), processed_data)

        # Train the FP-Growth model
        basket = all_processed_data.groupBy('ï»¿InvoiceNo', 'CustomerID') \
            .agg(collect_set('StockCode').alias('StockCodeList'))

        fp_growth = FPGrowth(itemsCol='StockCodeList', minSupport=0.025, minConfidence=0.3)
        model = fp_growth.fit(basket)

        # Get association rules
        association = model.associationRules
        
        

        print('Number of association rules:', association.count())
        print(association.show(10))

        item_ids = [22803, 82486]  # Example list of item_ids
        conditions = [array_contains(col("antecedent"), str(item_id)) for item_id in item_ids]
        filtered_associations = association.filter(reduce(lambda a, b: a | b, conditions))
        recommendations = filtered_associations.select("consequent", "confidence")

        sorted_recommendations = recommendations.orderBy("confidence", ascending=False)
        top_recommendations = sorted_recommendations.limit(10).collect()
        print("-----------------------------------------------------------")
        print("Top Recommendations", top_recommendations)
        print("-----------------------------------------------------------")
        consequent_stock_codes = [row["consequent"][0] for row in top_recommendations]

        # Filter data DataFrame to include only rows with consequent stock codes
        filtered_data = all_processed_data.filter(all_processed_data.StockCode.isin(consequent_stock_codes))

        # Retrieve descriptions of consequent stock codes from the filtered DataFrame
        consequent_descriptions = filtered_data.select("StockCode", "Description").distinct()

        # Display the descriptions of the consequent stock codes
        print(consequent_descriptions.show())

    spark.stop()

if __name__ == "__main__":
    consume_from_kafka_and_process()

