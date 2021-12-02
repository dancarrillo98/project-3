//Create a Producer for Qualified Lead Topic
//$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list sandbox-hdp.hortonworks.com:6667 --topic Qualified_Lead

//REMEMBER TO CREATE THE TOPIC FIRST:
//$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Qualified_Lead

//Run in maria_dev in another console to see total qualified leads
//spark-submit --packages org.apache.spark:spark-sql-fka-0-10_2.11:2.3.0  --class consumer.SparkConsumer  project-3_2.11-1.0.jar


package consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//object
class SparkConsumer{
    // def main(args: Array[String]): Unit = {
    //     val spark = SparkSession.builder.appName("Producer Test").config("spark.master", "local[*]").getOrCreate()

    //     import spark.implicits._

    //     spark.sparkContext.setLogLevel("ERROR")

    //     writeQualifiedLeadTotal(spark, "Qualified_Lead")
    //     //spark.streams.awaitAnyTermination()
    // }

    def writeQualifiedLeadTotal(spark: SparkSession, df: DataFrame): Unit = {
    //     //Subscribe to Qualified Lead Topic containing JSON data
    //    val df = spark.readStream
    //    .format("kafka")
    //    .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    //    .option("subscribe", topicName)
    //    .load()

    
        val dfSelect = df.select(col("key").cast("string"), col("value").cast("string"))
        dfSelect.printSchema()

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
       val qualifiedLeadDF = dfSelect.withColumn("JSON_data", from_json(col("value"), qualifiedLeadSchema)).select("JSON_data.*")
       qualifiedLeadDF.printSchema()

        //Write topic events to JSON file
       writeDataFrameToFile(qualifiedLeadDF)

       println("Total Number of Qualified Leads")
       //Select total number of Qualified Leads
       val qualifiedLeadCount = qualifiedLeadDF.select(count("id") as "total_qualified_leads")
       
       //Output result to console
       val outputResult = qualifiedLeadCount.writeStream
       .outputMode("complete")
       .format("console")
       .start()
       
       scala.io.StdIn.readLine()
       outputResult.stop()
    }

//SAMPLE OUTPUT
// root
//  |-- key: string (nullable = true)
//  |-- value: string (nullable = true)

// root
//  |-- id: integer (nullable = true)
//  |-- first_name: string (nullable = true)
//  |-- last_name: string (nullable = true)
//  |-- university: string (nullable = true)
//  |-- major: string (nullable = true)
//  |-- email: string (nullable = true)
//  |-- home_state: string (nullable = true)

// Total Number of Qualified Leads
// -------------------------------------------
// Batch: 0
// -------------------------------------------
// +---------------------+
// |total_qualified_leads|
// +---------------------+
// |                  156|
// +---------------------+

// -------------------------------------------
// Batch: 1
// -------------------------------------------
// +---------------------+
// |total_qualified_leads|
// +---------------------+
// |                  166|
// +---------------------+


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