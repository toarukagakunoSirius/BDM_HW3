import csv
import json
import numpy as np
import pandas as pd
import sys

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)
spark

products = spark.read.csv('/tmp/bdm/keyfood_products.csv', header=True).cache()
products = products.select("store", "department", F.split('upc','-')[1].alias('upc'), 
           F.regexp_extract('price','\d+\.\d+', 0).alias('price'))
sample = spark.read.csv('keyfood_sample_items.csv', header=True).cache()
sample = sample.select(F.split('UPC Code','-')[1].alias('upc'), "Item Name")

food = sample.join(products, ['upc'], how = 'left')

f = open('keyfood_nyc_stores.json')
data = json.load(f)
f.close()

dfS = [[data[x]['name'], data[x]['foodInsecurity']] for x in data]
store = spark.createDataFrame(pd.DataFrame(dfS, columns = ['name', 'foodInsecurity']))

inputTask1 = food.join(store, food.store == store.name, how = 'left')
outputTask1 = inputTask1.select(inputTask1['Item Name'], inputTask1['price'].alias('Price ($)').cast('float'),
              (inputTask1['foodInsecurity']*100).alias('% Food Insecurity').cast('int')).cache()

outputTask1.rdd.saveAsTextFile(sys.argv[1])