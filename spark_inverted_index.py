# spark_inverted_index.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from pymongo import MongoClient
from datetime import datetime
import re
import math
import os
import csv
import time

metrics_file = "metrics/indexing_metrics.csv"
start_time = time.time()

def preprocess_and_tokenize(content):
    # Tokenization and preprocessing logic
    if not content:
        return []
    
    # take to lower case
    content = content.lower()
    
    # use regex to clean 
    content = re.sub("' "," ",content)
    content = re.sub('"',"",content)
    content = re.sub("^'"," ",content)
    content = re.sub('^"',"",content)
    content = re.sub("[“.?\n!@#$%&()*:;/,<>-_+=”]"," ",content)
    content = re.sub(" +"," ",content)
    
    tokens = re.findall(r'\b\w+\b', content)
    return tokens

spark = SparkSession.builder \
    .appName("InvertedIndex") \
    .getOrCreate() 
sc = spark.sparkContext

# Load data from MongoDB
# client = MongoClient("localhost", 27017)
# Connect to the sharded cluster via mongos (port 27017).
client = MongoClient("mongodb://localhost:27017")

db = client['webcrawler']
docs = list(db['pages'].find({}, {"url": 1, "content": 1, "_id": 0}))
rdd = sc.parallelize(docs)

# cache the rdd
rdd.cache()

# find total num of docs
N = rdd.count()
print(f"Total number of documents: {N}")

# Compute TF for each doc
# gen initial map
tf_rdd = rdd.flatMap(lambda doc: [((word, doc['url']), 1) for word in preprocess_and_tokenize(doc.get('content', ''))]) 
# reduce to find count
tf_rdd = tf_rdd.reduceByKey(lambda a, b: a + b)

# Compute DF for each term
# gen initial map and then reduce to find count
df_rdd = tf_rdd.map(lambda x: (x[0][0], 1)).reduceByKey(lambda a, b: a + b)

# broadcast df_rdd to the workers
df_map_broadcast = sc.broadcast(df_rdd.collectAsMap())
print("Broadcasted df_rdd")
print("Vocabulary size:", len(df_map_broadcast.value))

# Compute IDF for each term
# make the inital dictionary with the necessary terms
tfidf_rdd = tf_rdd.map(lambda item: {'term': item[0][0], 'url': item[0][1], 'tf': item[1], 'df': df_map_broadcast.value[item[0][0]]})
# compute the idf
tfidf_rdd = tfidf_rdd.map(lambda item: {**item, 'idf': math.log(N /(1 + item['df']))})  
# compute tf-idf
tfidf_rdd = tfidf_rdd.map(lambda item: {**item, 'tfidf': item['tf'] * item['idf']})  
# remove df and tf
tfidf_rdd = tfidf_rdd.map(lambda item:(item['term'],(item['url'], item['tfidf'])))

print("TF-IDF computation completed")

# disable caching
rdd.unpersist()

# group into the final ii format 
final_ii = tfidf_rdd.groupByKey().mapValues(list)

# Save to file (or MongoDB)
final_ii.saveAsTextFile("inverted_index")


end_time = time.time()
time_taken = end_time - start_time

# Save to CSV
file_exist = os.path.isfile(metrics_file)
with open(metrics_file, mode='a', newline='') as file:
    writer = csv.writer(file)
    if not file_exist:
        writer.writerow(["timestamp", "time_taken", "docs_count", "terms_count"])
    writer.writerow([datetime.utcnow(), time_taken, N, final_ii.count()])

# Stop the Spark session
spark.stop()
