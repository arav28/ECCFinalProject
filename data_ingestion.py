#pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import col, explode
from pyspark.sql.functions import array_contains
from functools import reduce
from pyspark.sql.functions import col, collect_set
from pyspark.ml.fpm import FPGrowth

# Create a SparkSession
spark = SparkSession.builder \
    .appName("FP Growth Example") \
    .getOrCreate()

# Read the data into a Spark DataFrame
data = spark.read.csv('sample_data.csv', header=True, inferSchema=True, encoding='latin1')

# Drop rows with missing values
data = data.dropna()

# Add a column for total price
data = data.withColumn('GroupPrice', col('Quantity') * col('UnitPrice'))

# Display the dimensions and first few rows of the dataset
print('The dimensions of the dataset are:', (data.count(), len(data.columns)))
print('---------')
print(data.show(5))

# Convert StockCode to string
data = data.withColumn('StockCode', col('StockCode').cast('string'))

# Filter out rows with StockCode starting with non-numeric characters
data = data.filter(~col('StockCode').rlike('^[^0-9]'))

# Group data by InvoiceNo and CustomerID, and aggregate StockCode into a list
basket = data.groupBy('InvoiceNo', 'CustomerID') \
    .agg(collect_set('StockCode').alias('StockCodeList'))

# Display dimensions and first few rows of the grouped dataset
print('Dimensions of the new grouped dataset:', (basket.count(), len(basket.columns)))
print('---------')
print(basket.show(5))

# Define the FP-Growth model
fp_growth = FPGrowth(itemsCol='StockCodeList', minSupport=0.025, minConfidence=0.3)

# Train the FP-Growth model
model = fp_growth.fit(basket)

# Get association rules
association = model.associationRules

# Explode the antecedent array column to individual elements
association_exploded = association.withColumn('antecedent_exploded', explode('antecedent'))

# Join with original data to get descriptions
association_with_desc = association_exploded.join(data.select("StockCode", "Description"),
                                                  association_exploded.antecedent_exploded == data.StockCode,
                                                  "inner").drop("StockCode")

# Display association rules
print('Number of association rules:', association.count())
print(association.show(10))

item_ids = ["84406B","71053"]  # Example list of item_ids|
conditions = [array_contains(col("antecedent"), str(item_id)) for item_id in item_ids]
filtered_associations = association.filter(reduce(lambda a, b: a | b, conditions))
recommendations = filtered_associations.select("consequent", "confidence")

sorted_recommendations = recommendations.orderBy("confidence", ascending=False)
top_recommendations = sorted_recommendations.limit(10).collect()
print(top_recommendations)

consequent_stock_codes = [row["consequent"][0] for row in top_recommendations]

# Step 2: Filter data DataFrame to include only rows with consequent stock codes
filtered_data = data.filter(data.StockCode.isin(consequent_stock_codes))

# Step 3: Retrieve descriptions of consequent stock codes from the filtered DataFrame
consequent_descriptions = filtered_data.select("StockCode", "Description").distinct()

# Display the descriptions of the consequent stock codes
print(consequent_descriptions.show())

