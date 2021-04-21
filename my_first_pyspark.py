# Databricks notebook source
#Run new community databrick cluster

# COMMAND ----------

#Create new w RDD object. It's a list with 3 values.

# COMMAND ----------

rdd = sc.parallelize([
    "this is positive message",
    "this is negative comment",
    "very positive and happy"
])

# COMMAND ----------

#simple_sentiment function is a mapping function. It assign words to the sentiment. 

# COMMAND ----------

def simple_sentiment(text):
    if "positive" in text:
        return "positive"
    else:
        return "negative"

# COMMAND ----------

# fun is a reduction function that counts sentiment values of the analysis.

# COMMAND ----------

def fun(a):
    return (simple_sentiment(a), 1)

rdd.map(fun).reduceByKey(lambda a,b: a+b).take(10)
