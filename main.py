import findspark
SPARK_HOME = r"C:\Users\jmjl\Desktop\Master\BigData\spark\spark-3.0.2-bin-hadoop2.7"
findspark.init(spark_home=SPARK_HOME)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
# from textblob import TextBlob


consumer_key = "Hs8q9Y5RnQdW5twdX3Y24adoD"
consumer_secret = "W5kVjmMTyqafEPg8VIVKpkzHDJqmTiT58EHdNkKVYpWcI8vxYQ"
access_token = '1333741454217375746-mqcMEgWznro7BSL6s8Pl0JSJ07xP6l'
access_secret = 'BGMTw0L8Km9aU1Nt2PgbWT6agYBUdQei4dWecEEm2cInP'


def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )


spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

quiet_logs(spark)

lines = spark.readStream.format('socket').option('host', 'localhost').option('port', 9999).load()

query = lines.writeStream.outputMode("update").format("console").start()
query.awaitTermination()

