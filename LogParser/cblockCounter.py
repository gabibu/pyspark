from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.context import SparkContext
from pyspark.sql import HiveContext
import sys

LOG_REGEX = '(.*?) (.*?) (\d{1,3}\.\d{1,3}\.\d{1,3})(\.\d{1,3}):([0-9][0-9]{3,4}) (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):([0-9][0-9]{1,4}) (.*?) (.*?) (.*?) ([1-9][0-9]{2}) ([1-9][0-9]{2}) ([0-9]+) ([0-9]+) \\\"(.*?)\\\" \\\"(.*?)\\\" (.*?) (.*?)'
NUM_OF_ROWS_IN_FILE = 45000.0
def sequenceToCsv(sequence):
    return  ','.join([str(elem) for elem in sequence])


def collectCblockAgencts(inPath, cblockCounterPath, cblockCounterUAPath):

    sc = SparkContext('local', 'test')
    spark = HiveContext(sc)

    collectToCSv = udf(sequenceToCsv, StringType())


    lines = spark.read.text(inPath).rdd.map(lambda r:r[0])

    regexBroadcast = sc.broadcast(LOG_REGEX)


    def runREgex(line):
        import re
        regex = regexBroadcast.value
        values = re.match(regex, line).groups()

        if len(values) == 18:
            return values[2], values[15]
        else:
            raise Exception('regex {0} didnt match line {1} '.format(regex, line))


    blockAgenctPair = lines.map(runREgex)

    cblockUserAgenct = spark.createDataFrame(blockAgenctPair, schema=['block', 'agent'])


    cblockUserAgenct.createTempView('logBlockUA')

    blockData = \
        spark.sql('select block, count(*) as counter, collect_set(agent) as UASet  from logBlockUA group by block')


    blockDataWithAgenctsCSv = \
        blockData.withColumn("UA", collectToCSv(blockData.UASet))

    blockDataWithAgenctsCSv = blockDataWithAgenctsCSv.drop('UASet')

    blockDataWithAgenctsCSv = blockDataWithAgenctsCSv[['block', 'counter', 'UA']]

    blockDataWithAgenctsCSv.persist(StorageLevel.MEMORY_AND_DISK_2)

    numOfRows = blockDataWithAgenctsCSv.count()

    numOfFiles = max(int( numOfRows / NUM_OF_ROWS_IN_FILE), 1)

    blockDataWithAgenctsCSv.drop('UA').repartition(numOfFiles). \
        write.mode('overwrite').format("com.databricks.spark.csv").option("header", "true").save(cblockCounterPath)



    blockDataWithAgenctsCSv.repartition(numOfFiles). \
        write.mode('overwrite').format("com.databricks.spark.csv").option("header", "true").save(cblockCounterUAPath)




if __name__ == "__main__":
    #paramaters
    #  1 -> input path
    #  2 2.a output path
    #  3 2.b output path

    inPath = sys.argv[1]
    cblockCounterPath = sys.argv[2]
    cblockCounterUAPath = sys.argv[3]

    collectCblockAgencts(inPath, cblockCounterPath, cblockCounterUAPath)
