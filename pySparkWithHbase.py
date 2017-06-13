input_table = "InputTable"
output_table = "OutputTable"

def process_data(inputRows):
    for data in inputRows: #Iterate Row over hbase
        system_id = data["system_id"]
        #Process your data
        #yield [(input_table,[str(system_id),"f","Column Families", value])] save data in input table
        yield (output_table,[rowKey,"Column Families","key", value])  #save data in output table
        # Or you can Return List of Tuple
     
def save_record(rdd):
    host = '172.**.**.51'
    keyConv1 = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv1 = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    conf = {"hbase.zookeeper.quorum": host,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    rdd.saveAsNewAPIHadoopDataset(keyConverter=keyConv1,valueConverter=valueConv1,conf=conf)

def filter_rows(x):
    #filter rows according to your requirement

def RunProcessWithSpark():
    from pyspark import SparkContext
 
    sc = SparkContext(appName="samplePysparkHbase", pyFiles=['/home/centos/pythonFile.py'])
 
    conf = {"hbase.zookeeper.quorum": "172.**.**.51", "hbase.mapreduce.inputtable": input_table}
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

    hbaseRdd = sc.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter=keyConv,
        valueConverter=valueConv,
        conf=conf)

    messageRdd = hbaseRdd.map(lambda x: x[1])  # message_rdd = hbase_rdd.map(lambda x:x[0]) will give only row-key
    filterRdd = rdd.filter(lambda x : filter_rows(x)) # Filter Hbase data if required
    processedRdd = filterRdd.mapPartitions(lambda x: process_data(x)) # Process data
    processedRdd = processedRdd.flatMap(lambda x: x)

    save_record(processedRdd) # Save process data to hbase


if __name__ == '__main__':
    RunProcessWithSpark()




