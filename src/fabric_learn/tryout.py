# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.7
#   kernelspec:
#     display_name: fabric-learn-IXct3f4y-py3.13
#     language: python
#     name: python3
# ---

import pyspark
from delta import *

LAKE = "/Users/andy/data/local/"

# +
builder = pyspark.sql.SparkSession.builder.appName("tryout") \
    .config("spark.sql.extensions", 
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", 
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
# -

data = spark.range(0, 5)
data.write.format("delta").save(f"{LAKE}delta-table", mode="overwrite")

df = spark.read.format("delta").load(f"{LAKE}delta-table")
df.show()

shipment_prices = spark.read.format("csv").load(
    f"{LAKE}SCMS_Delivery_History_Dataset_20150929.csv", header=True, inferSchema=True)

shipment_prices.head(20)

rows = shipment_prices.collect()

dict_list = [row.asDict() for row in rows]
dict_list

# https://delta.io/learn/getting-started/
# See for streaming example


