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
    qualifiedLeadSpark.writeQualifiedLeadTotal(spark, topic6)
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
  
    val screeningTopicDF = getValueDF(topic5)
    println("Schema of Screeners Data Stream")
    topic5.printSchema()
    // Recruiters Schema
    val screeningSchema = new StructType()
        .add("screener_id", IntegerType, false)
        .add("ql_id", IntegerType, false)
        .add("screening_date", DateType, false)
        .add("start_time", StringType, false)
        .add("end_time", StringType, false)
        .add("screening_type", StringType, false)
        .add("question_number", IntegerType, false)
        .add("question_accepted", IntegerType, false)
    // Extract json Data from topic input in column 'value'
    val screeningDF = changeSchema(screeningTopicDF, screeningSchema)
    println("Updated Screening DataFrame Schema")
    screeningDF.printSchema()
    // Query for count of all contact attempts, output to console in complete mode to show all results after data is published to contact attempts topic
    val allCountQuery = screeningDF.select(count("screener_id") as "Number of Screeners").writeStream
      .outputMode("complete")
      .format("console")
      .start()
    // Query for count of contact attempts per recruiter
    val attemptsPerScreenerQuery =  screeningDF.groupBy("screener_id").count().orderBy(col("count").desc).writeStream
      .format("console")
      .start()
    scala.io.StdIn.readLine("Showing Results\nPress Enter to Return to Main Menu\n")
    allCountQuery.stop()
    attemptsPerScreenerQuery.stop()
  
  }

  def q4(): Unit = {
 
  }
}
