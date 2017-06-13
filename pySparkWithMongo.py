from bson import ObjectId
from pymongo import MongoClient





def get_mongo_collection():
    c  = MongoClient('localhost',27017)
    db = c.DataBase
    collection = db.Collecton
    return collection

def insert_data_into_collection(collection, dictionary):
    collection.insert_one(dictionary)


def process_data(document, broadcastVariableOne):
    #process data and save data to mongo 
    insert_data_into_collection(processDict)


def RunProcessWithSpark():
    import pymongo_spark
    from pyspark import SparkContext, SparkConf
    pymongo_spark.activate()
    conf = (SparkConf().setAppName("AppName"))
        
    sc = SparkContext(conf=conf,pyFiles="PythonFiles")
        
    #setting up RDD    
    rdd = sc.mongoRDD(config.dbConnectionString)
    #filterRDD = rdd.filter(lambda x : x if x.has_key("FilterKey")) # Filter RDD if Required
    
    broadcastVariableOne =sc.broadcast(Variable) # Broadcast variable, you can create according your requirement
    filterRDD.foreach(  lambda x: process_data(x,broadcastVariableOne.value)
      


if __name__ == '__main__':
    RunProcessWithSpark()  





