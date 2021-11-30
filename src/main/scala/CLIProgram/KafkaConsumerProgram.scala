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


class  KafkaConsumerProgram extends Thread{  
  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("firstProducerProgram")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val topic1 = sparkStream(spark, "test_topic")
  val topic2 = sparkStream(spark, "recruiters_test")
  val topic3 = sparkStream(spark, "test_topic")
  val topic4 = sparkStream(spark, "contactAttempts_test")
  val topic5 = sparkStream(spark, "test_topic")
  val topic6 = sparkStream(spark, "test_topic")


  def sparkStream(spark: SparkSession, topic: String): DataFrame = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
    return df
  }

/* 

    def consumeFromKafka(topic: String)= {
        val props: Properties = new Properties()
        props.put("group.id", "main")
        props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
         props.put(
                    "key.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer"
        )
        props.put(
                    "value.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer"
        )
        props.put("enable.auto.commit", "true")
        props.put("auto.commit.interval.ms", "1000")
        // Define properties

        val consumer = new KafkaConsumer(props)

        try {
            consumer.subscribe(util.Arrays.asList(topic))
            while (true) {
                val records = consumer.poll(10)
                for (record <- records.asScala) {
                // Do Stuff
                }
            }
       } catch {
            case e: Exception => e.printStackTrace()
           } finally {
            consumer.close()
           }
    }

 */

  def q1(): Unit = {
 
  }

  def q2(): Unit = {
    val recruiterTopicDF = topic2
      .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
      .select("key","value")

    val contactAttemptsTopicDF = topic4
      .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
      .select("key","value")
    
    topic2.printSchema()
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
    val recruitersDF = recruiterTopicDF.withColumn("jsonData", from_json(col("value"),recruiterSchema)).select("jsonData.*")
    recruitersDF.printSchema()
    val contactAttemptsDF = contactAttemptsTopicDF.withColumn("jsonData", from_json(col("value"),contactAttemptSchema)).select("jsonData.*")
    contactAttemptsDF.printSchema()
    // Query for count of all contact attempts, output to console in complete mode to show all results after data is published to contact attempts topic
    val allCountQuery = contactAttemptsDF.select(count("*") as "count").writeStream
      .outputMode("complete")
      .format("console")
      .start()
    // Query for count of contact attempts per recruiter
    val countByRecruiterQuery = contactAttemptsDF.groupBy("recruiter_id").count().writeStream
      .outputMode("complete")
      .format("console")
      .start()
    scala.io.StdIn.readLine()
    allCountQuery.stop()
    countByRecruiterQuery.stop()
  }

  def q3(): Unit = {
  
  }

  def q4(): Unit = {
 
  }
}
