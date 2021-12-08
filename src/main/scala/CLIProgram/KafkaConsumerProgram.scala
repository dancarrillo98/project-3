package CLIProgram

/* import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._
import java.time.Duration */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import consumer.SparkConsumer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

class  KafkaConsumerProgram extends Thread{  
  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("firstProducerProgram")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val topic1 = sparkStream(spark, "Screeners")
  val topic2 = sparkStream(spark, "Recruiters")
  val topic3 = sparkStream(spark, "Qualified_Lead")
  val topic4 = sparkStream(spark, "Contact_Attempts")
  val topic5 = sparkStream(spark, "Screening")
  val topic6 = sparkStream(spark, "Offers")



  def sparkStream(spark: SparkSession, topic: String): DataFrame = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
    return df
  }

  def getValueDF(topic: DataFrame): DataFrame = {
      val newDF = topic
      .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
      .select("key","value")
      return newDF
  }

  // Extract json Data from topic input in column 'value'
  def changeSchema(df: DataFrame, schema: StructType): DataFrame = {
    val newDF = df.withColumn("jsonData", from_json(col("value"),schema)).select("jsonData.*")
    return newDF
  }


 //Determine and display on the console the total number of Qualified Leads
  def q1(): Unit = {
    val qualifiedLeadSpark = new SparkConsumer()
    qualifiedLeadSpark.writeQualifiedLeadTotal(topic3)
  }

  def q2(): Unit = {
    val recruiterTopicDF = getValueDF(topic2)
    val contactAttemptsTopicDF =  getValueDF(topic4)
    println("Schema of Reruiters Data Stream")
    topic2.printSchema()
    println("Schema of Contact_Attempts Data Stream")
    topic4.printSchema()
    // Recruiters Schema
    val recruiterSchema = new StructType()
        .add("id", IntegerType, false)
        .add("first_name", StringType, false)
        .add("last_name", StringType, false)
    // Contact Attempts Schema
    val contactAttemptSchema = new StructType()
        .add("recruiter_id", IntegerType, false)
        .add("ql_id", IntegerType, false)
        .add("contact_date", DateType, false)
        .add("start_time", StringType, false)
        .add("end_time", StringType, false)
        .add("contact_method", StringType, false)
    // Extract json Data from topic input in column 'value'
    val recruitersDF = changeSchema(recruiterTopicDF,recruiterSchema)
    println("Updated Recruiters DataFrame Schema")
    recruitersDF.printSchema()
    val contactAttemptsDF = changeSchema(contactAttemptsTopicDF,contactAttemptSchema)
    println("Updated Contact_Attempts DataFrame Schema")
    contactAttemptsDF.printSchema()
    // Query for count of all contact attempts, output to console in complete mode to show all results after data is published to contact attempts topic
    val allCountQuery = contactAttemptsDF.select(count("ql_id") as "Number of Contact Attempts").writeStream
      .outputMode("complete")
      .format("console")
      .start()
    // Query for count of contact attempts per recruiter
    val countByRecruiterQuery = contactAttemptsDF.groupBy("recruiter_id").count().orderBy(col("count").desc).writeStream
      .outputMode("complete")
      .format("console")
      .start()
    scala.io.StdIn.readLine("Showing Results\nPress Enter to Return to Main Menu\n")
    allCountQuery.stop()
    countByRecruiterQuery.stop()
  }

  def q3(): Unit = {
  
  }

  def q4(): Unit = {
 
  }

  //Filepaths for Writing Topic Events to Files
  val topic1filepath = "/user/maria_dev/screeners"
  val topic2filepath = "/user/maria_dev/recruiters"
  val topic3filepath = "/user/maria_dev/qualifiedLead"
  val topic4filepath = "/user/maria_dev/contactAttempts"
  val topic5filepath = "/user/maria_dev/screening"
  val topic6filepath = "/user/maria_dev/offers"

  //Checkpoints for Writing Topic Events to Files
  val topic1checkpoint = "file:///home/maria_dev/checkpoint1"
  val topic2checkpoint = "file:///home/maria_dev/checkpoint2"
  val topic3checkpoint = "file:///home/maria_dev/checkpoint3"
  val topic4checkpoint = "file:///home/maria_dev/checkpoint4"
  val topic5checkpoint = "file:///home/maria_dev/checkpoint5"
  val topic6checkpoint = "file:///home/maria_dev/checkpoint6"

  //Write the streaming DataFrames from the Topics to files 
  //Executed at the startup of the application
  def writeTopicsToFile(): Unit = {
    //Screeners
    val screenerSchema = new StructType()
      .add("id", IntegerType, false)
      .add("first_name", StringType, false)
      .add("last_name", StringType, false)
    writeTopicToFile(topic1, screenerSchema, topic1filepath, topic1checkpoint)

    //Recruiters
    val recruiterSchema = new StructType()
      .add("id", IntegerType, false)
      .add("first_name", StringType, false)
      .add("last_name", StringType, false)
    writeTopicToFile(topic2, recruiterSchema, topic2filepath, topic2checkpoint)

    //Qualified_Lead
    val qualifiedLeadSchema = new StructType()
      .add("id", IntegerType, false)
      .add("first_name", StringType, false)
      .add("last_name", StringType, false)
      .add("university", StringType, false)
      .add("major", StringType, false)
      .add("email", StringType, false)
      .add("home_state", StringType, false)
    writeTopicToFile(topic3, qualifiedLeadSchema, topic3filepath, topic3checkpoint)

    //Contact_Attempts
    val contactAttemptSchema = new StructType()
      .add("recruiter_id", IntegerType, false)
      .add("ql_id", IntegerType, false)
      .add("contact_date", StringType, false)
      .add("start_time", StringType, false)
      .add("end_time", StringType, false)
      .add("contact_method", StringType, false)
    writeTopicToFile(topic4, contactAttemptSchema, topic4filepath, topic4checkpoint)

    //Screening
    val screeningSchema = new StructType()
      .add("screener_id", IntegerType, false)
      .add("ql_id", IntegerType, false)
      .add("screening_date", StringType, false)
      .add("start_time", StringType, false)
      .add("end_time", StringType, false)
      .add("screening_type", StringType, false)
      .add("question_number", IntegerType, false)
      .add("question_accepted", IntegerType, false)
    writeTopicToFile(topic5, screeningSchema, topic5filepath, topic5checkpoint)

    //Offers
    val offerSchema = new StructType()
      .add("screener_id", IntegerType, false)
      .add("recruiter_id", IntegerType, false)
      .add("ql_id", IntegerType, false)
      .add("offer_extended_date", StringType, false)
      .add("offer_action_date", StringType, false)
      .add("contact_method", StringType, false)
      .add("offer_action", StringType, false)
    writeTopicToFile(topic6, offerSchema, topic6filepath, topic6checkpoint)
  }

  //Helper function to writeTopicsToFile function
  def writeTopicToFile(topic: DataFrame, schema: StructType, topicFilePath: String, topicCheckpoint: String): Unit = {
    val topicWriter = new SparkConsumer()
    val df = topic.select(col("value").cast("string"))
    val updatedDF = changeSchema(df, schema)
    topicWriter.writeDataFrameToFile(updatedDF, topicFilePath, topicCheckpoint)
  }

  //Merge all the files written from each topic into single files 
  //Executed after manually exiting the program by pressing 5 in the main menu
  def mergeFiles(): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    
    var srcPath = new Path(topic1filepath)
    for (i <- 1 to 6){
      i match{
        case 1 => srcPath = new Path(topic1filepath)
        case 2 => srcPath = new Path(topic2filepath)
        case 3 => srcPath = new Path(topic3filepath)
        case 4 => srcPath = new Path(topic4filepath)
        case 5 => srcPath = new Path(topic5filepath)
        case 6 => srcPath = new Path(topic6filepath)
      }
      
      val destPath = new Path(s"topic${i}.json")
      if (hdfs.exists(destPath)){
        hdfs.delete(destPath)
      }
      if (hdfs.exists(srcPath)){
        FileUtil.copyMerge(hdfs, srcPath, hdfs, destPath, false, hadoopConfig, null)
      }
      
    }
  }
}
