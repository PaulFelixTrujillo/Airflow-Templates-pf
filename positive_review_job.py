import findspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import StopWordsRemover
import pyspark.sql.functions as f

findspark.init()
spark = SparkSession.builder.getOrCreate()

df = (spark.read 
    .format("jdbc") 
    .option("url", "jdbc:postgresql://34.69.141.47/postgres") 
    .option("dbtable", "public.movie_review_dt") 
    .option("user", "postgres")
    .option("password", "postgres") 
    .option("driver", "org.postgresql.Driver")
    .load())

tokenizer = Tokenizer(inputCol="review_str", outputCol="review_token")
tokenized = tokenizer.transform(df).select('cid','review_token')



remover = StopWordsRemover(inputCol='review_token', outputCol='token_clean')
data_clean = remover.transform(tokenized).select('cid', 'token_clean')



dfs=data_clean.withColumn("positive_review",f.array_contains(f.col("token_clean"),"good").cast('integer'))


write_df = dfs.select("cid", "positive_review")

table_name = "reviews"
(write_df.write 
    .format("jdbc") 
    .option("url", "jdbc:postgresql://34.69.141.47/postgres") 
    .option("dbtable", f"public.{table_name}") 
    .option("user", "postgres")
    .option("password", "postgres") 
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .option("truncate", True)
    .save())