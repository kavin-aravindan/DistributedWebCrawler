# spark_inverted_index.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col
from pymongo import MongoClient

spark = SparkSession.builder \
    .appName("InvertedIndex") \
    .getOrCreate()

# Load data from MongoDB
client = MongoClient("localhost", 27017)
db = client['webcrawler']
docs = list(db['pages'].find({}, {"url": 1, "content": 1, "_id": 0}))
rdd = spark.sparkContext.parallelize(docs)

# Build inverted index
inverted = (
    rdd.flatMap(lambda doc: [(word.lower(), doc['url']) for word in doc['content'].split()])
        .distinct()
        .groupByKey()
        .mapValues(list)
)

# Save to file (or MongoDB)
inverted.saveAsTextFile("inverted_index")
