package consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkConsumer{
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder.appName("Producer Test").config("spark.master", "local[*]").getOrCreate()

        import spark.implicits._

        spark.sparkContext.setLogLevel("ERROR")

        writeQualifiedLeadTotal(spark, "Qualified_Lead")
        spark.streams.awaitAnyTermination()
    }

    def writeQualifiedLeadTotal(spark: SparkSession, topicName: String): Unit = {
        //Subscribe to Qualified Lead Topic containing JSON data
       val df = spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
       .option("subscribe", topicName)
       .load()
       .select(col("key").cast("string"), col("value").cast("string"))
       df.printSchema()

       //Schema for Qualified Leads
       val qualifiedLeadSchema = new StructType()
        .add("id", IntegerType, false)
        .add("first_name", StringType, false)
        .add("last_name", StringType, false)
        .add("university", StringType, false)
        .add("major", StringType, false)
        .add("email", StringType, false)
        .add("home_state", StringType, false)

        //Apply schema to DF containing JSON data
       val qualifiedLeadDF = df.withColumn("JSON_data", from_json(col("value"), qualifiedLeadSchema)).select("JSON_data.*")
       qualifiedLeadDF.printSchema()

        //Write topic events to JSON file
       writeDataFrameToFile(qualifiedLeadDF)

       println("Total Number of Qualified Leads")
       //Select total number of Qualified Leads
       val qualifiedLeadCount = qualifiedLeadDF.select(count("id") as "total_qualified_leads")
       
       //output result to console
       val outputResult = qualifiedLeadCount.writeStream
       .outputMode("complete")
       .format("console")
       .start()
       
    }

    //Call this function using DF with schema as parameter to store events into JSON file
    def writeDataFrameToFile(df: DataFrame): Unit = {
        //TODO
        //Need to decide where to put these filepath variables
        //Change filename path after testing

        //Filepath
        val filePath = "file:///home/maria_dev/json_data"
        //Checkpoint Path
        val checkPointPath = "file:///home/maria_dev/checkpoint"

        //Write DF to JSON file
        df.writeStream
        .outputMode("append")
        .format("json")
        .option("path", filePath)
        .option("checkpointLocation", checkPointPath)
        .start()
    }
}