import cml.data_v1 as cmldata

CONNECTION_NAME = "go01-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# Create DB for Jet Engine

db="jetengine"
spark.sql("create database if not exists " + db)


# Load & Create tables
import os 
import pandas as pd

for fname in  os.listdir("data"):
    
    if ( fname.split('.')[1] == 'csv' ):
      tbname = fname.split('.')[0]
      fpath = "data/" + fname

      print ("Fichier : " + fname)
      print ("Table   : " + tbname)
    
      df=pd.read_csv(fpath , header='infer', sep=',')
             
      sf=spark.createDataFrame(df)
      sf.write.format("parquet").mode("overwrite").saveAsTable(db + "." + tbname)



