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

  //Filepaths for Writing
  val topic1filepath = "/user/maria_dev/screeners"
  val topic2filepath = "/user/maria_dev/recruiters"
  val topic3filepath = "/user/maria_dev/qualifiedLead"
  val topic4filepath = "/user/maria_dev/contactAttempts"
  val topic5filepath = "/user/maria_dev/screening"
  val topic6filepath = "/user/maria_dev/offers"

  //Checkpoints for Writing
  val topic1checkpoint = "file:///home/maria_dev/checkpoint1"
  val topic2checkpoint = "file:///home/maria_dev/checkpoint2"
  val topic3checkpoint = "file:///home/maria_dev/checkpoint3"
  val topic4checkpoint = "file:///home/maria_dev/checkpoint4"
  val topic5checkpoint = "file:///home/maria_dev/checkpoint5"
  val topic6checkpoint = "file:///home/maria_dev/checkpoint6"

  //Write the streaming DataFrames from the Topics to files
  def writeTopicsToFile(): Unit = {

    //Screeners
    //topicWriter.writeDataFrameToFile(spark, topic1, "")
    writeScreenersToFile()

    //Recruiters
    writeRecruitersToFile()

    //Qualified_Lead
    writeQualifiedLeadToFile()

    //Contact_Attempts
    writeContactAttemptsToFile()

    //Screening
    //topicWriter.writeDataFrameToFile(spark, topic5, "")
    writeScreeningToFile()

    //Offers
    //topicWriter.writeDataFrameToFile(spark, topic6, "")
    writeOffersToFile()
  }

  def writeScreenersToFile(): Unit = {
    val topicWriter = new SparkConsumer()
    val df = topic1.select(col("value").cast("string"))

    //Need schema
  }

  def writeRecruitersToFile(): Unit = {
    val topicWriter = new SparkConsumer()
    val df = topic2.select(col("value").cast("string"))

    val recruiterSchema = new StructType()
      .add("id", IntegerType, false)
      .add("first_name", StringType, false)
      .add("last_name", StringType, false)

    val recruitersDF = changeSchema(df,recruiterSchema)
    topicWriter.writeDataFrameToFile(spark, recruitersDF, topic2filepath, topic2checkpoint)
  }

  def writeQualifiedLeadToFile(): Unit = {
    val topicWriter = new SparkConsumer()
    val df = topic3.select(col("value").cast("string"))

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
    val qualifiedLeadDF = changeSchema(df, qualifiedLeadSchema)
    topicWriter.writeDataFrameToFile(spark, qualifiedLeadDF, topic3filepath, topic3checkpoint)
  }

  def writeContactAttemptsToFile(): Unit = {
    val topicWriter = new SparkConsumer()
    val df = topic4.select(col("value").cast("string"))

    val contactAttemptSchema = new StructType()
      .add("recruiter_id", IntegerType, false)
      .add("ql_id", IntegerType, false)
      .add("contact_date", DateType, false)
      .add("start_time", StringType, false)
      .add("end_time", StringType, false)
      .add("contact_method", StringType, false)

    val contactAttemptsDF = changeSchema(df,contactAttemptSchema)
    topicWriter.writeDataFrameToFile(spark, contactAttemptsDF, topic4filepath, topic4checkpoint)
  }

  def writeScreeningToFile(): Unit = {
    val topicWriter = new SparkConsumer()
    val df = topic5.select(col("value").cast("string"))

    //Need schema
  }

  def writeOffersToFile(): Unit = {
    val topicWriter = new SparkConsumer()
    val df = topic6.select(col("value").cast("string"))
    
    //Need schema
  }

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
        FileUtil.copyMerge(hdfs, srcPath, hdfs, destPath, true, hadoopConfig, null)
        hdfs.delete(srcPath, true)
      }
      
    }
  }
}
