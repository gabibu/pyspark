
from datetime import datetime


import os
import re
import datetime

def loadDBFromBaseFolder(spark):
    if 'DB_FOLDER_PATH' in os.environ:
        folder = os.environ['DB_FOLDER_PATH']
        createDB(folder, spark)
    else:
        raise Exception('no DB_FOLDER_PATH environment variable')


def createDB(baseFolder, spark):

    if not os.path.isdir(baseFolder):
        raise Exception('{baseFolder} does not exist'.format(baseFolder=baseFolder))

    dbsFolders = os.listdir(baseFolder)

    # from pyspark.sql import SparkSession
    # spark = SparkSession.builder.appName('Test').enableHiveSupport().getOrCreate()

    for dbFolderPath in dbsFolders:

        dbName  = re.findall('db=(\w*)', dbFolderPath)[0]

        spark.sql('create database IF NOT EXISTS  {0}'.format(dbName))
        spark.sql('use {0}'.format(dbName))

        dbFolderFullPath = os.path.join(baseFolder, dbFolderPath)
        dbTablesFolders = os.listdir(dbFolderFullPath)

        existingTables = ['{0}.{1}'.format(table.database, table.name) for table in spark.catalog.listTables()]

        for tableFolder in dbTablesFolders:
            tableFolderFullPath = os.path.join(dbFolderFullPath, tableFolder)

            tableName = re.findall('table=(\w*)', tableFolder)[0]

            if '{0}.{1}'.format(dbName, tableName) in existingTables:
                spark.sql('DROP table  IF EXISTS {0}.{1}'.format(dbName, tableName))

            currentDF = spark.read.parquet(tableFolderFullPath)

            tempTableName = 'temp_{0}_{1}'.format(tableName, datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S'))
            currentDF.createTempView(tempTableName)

            spark.sql("create table  {0}.{1}  as select * from {2}".format(dbName, tableName, tempTableName))


