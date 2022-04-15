import csv
import sys
import json
import numpy as np
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)
spark
keyfood_nyc_stores = spark.read.json("keyfood_nyc_stores.json")
keyfood_nyc_stores = keyfood_nyc_stores.toPandas()
list_name = []
list_foodInsecurity = []
for column in keyfood_nyc_stores:
  list_name.append(column)
  list_foodInsecurity.append(keyfood_nyc_stores[column][0]['foodInsecurity'])
short_dict = dict(zip(list_name, list_foodInsecurity))
keyfood_sample_items = spark.read.csv('keyfood_sample_items.csv', header=True, escape='"')
keyfood_sample_items = keyfood_sample_items.withColumn("numberic_upc", F.split('UPC code', '-')[1])
keyfood_sample_items = keyfood_sample_items.withColumnRenamed("Item Name","item_name").cache()
keyfood_products = spark.read.csv('/tmp/bdm/keyfood_products.csv', header=True, escape='"')
keyfood_products = keyfood_products.select('store', 'upc', 'product', 'price').dropna()
keyfood_products = keyfood_products.withColumn("numberic_upc", F.split('upc', '-')[1]).cache()
merge_keyfood_product = keyfood_sample_items.join(keyfood_products, (keyfood_sample_items.numberic_upc ==  keyfood_products.numberic_upc),"inner")
merge_keyfood_product = merge_keyfood_product.select('store', 'upc', 'product', 'price').cache()
merge_keyfood_product = merge_keyfood_product.withColumn("price", F.regexp_extract("price", '([0-9]+[.][0-9]+)', 0))
merge_keyfood_product = merge_keyfood_product.withColumn("price",merge_keyfood_product.price.cast("float"))
from pyspark.sql.types import StringType
def translate(mapping):
    def translate_(col):
        return mapping.get(col)
    return F.udf(translate_, StringType())

merge_keyfood_product = merge_keyfood_product.withColumn("foodInsecurity", translate(short_dict)("store"))
merge_keyfood_product = merge_keyfood_product.withColumn("% Food Insecurity",100 * F.round(merge_keyfood_product.foodInsecurity.cast("float"),2))
merge_keyfood_product = merge_keyfood_product.withColumnRenamed("product", "Item Name")
merge_keyfood_product = merge_keyfood_product.withColumnRenamed("price", "Price ($)")
merge_keyfood_product = merge_keyfood_product.select('Item Name', 'Price ($)', '% Food Insecurity').cache()
merge_keyfood_product.rdd.saveAsTextFile(sys.argv[1])
