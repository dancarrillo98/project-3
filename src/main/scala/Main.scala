import consumer.PrintConsumer.printTopicData
import consumer.SparkConsumer.displayQualifiedLeadTotal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset

object Main  {
    def main(args: Array[String]): Unit = {
            val spark = SparkSession.builder
            .appName("Project 3")
            .config("spark.master", "local[*]")
            .getOrCreate()

        import spark.implicits._

        spark.sparkContext.setLogLevel("ERROR")

        val qualifiedLeadSchema = StructType(
            List(
                StructField("id", IntegerType, true), 
                StructField("first_name", StringType, true), 
                StructField("last_name", StringType, true), 
                StructField("university", StringType, true), 
                StructField("major", StringType, true), 
                StructField("email", StringType, true), 
                StructField("home_state", StringType, true)
                )
            )

        val initDF = spark.readStream.format("csv")
            .option("maxFilesPerTrigger", 2)
            .option("header", true)
            .option("path", "data/stream")
            .schema(qualifiedLeadSchema)
            .load()
        initDF.printSchema()
        
        val resultDF = initDF.withColumn("value", concat_ws("|", col("id"), col("first_name"), col("last_name"), col("university"), col("major"), col("email"), col("home_state")))
            .withColumn("key", col("id"))
            .drop("id").drop("first_name")
            .drop("last_name")
            .drop("university")
            .drop("major")
            .drop("email")
            .drop("home_state")
        resultDF.printSchema()

        resultDF.selectExpr("CAST (key AS STRING) AS key", "CAST(value AS STRING) AS value")
            .writeStream.format("kafka")
            .outputMode("append")
            .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
            .option("topic", "test_topic")
            .option("checkpointLocation", "file:///home/maria_dev/output/checkpoint/filesink_checkpoint")
            .start()


        val inputDF = spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
            .option("subscribe", "test_topic")
            .load()
            .select(col("value").cast("string"))

        printTopicData(inputDF, spark)
    }
}