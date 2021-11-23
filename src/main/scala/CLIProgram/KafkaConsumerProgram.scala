package CLIProgram

/* import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._
import java.time.Duration */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame


class  KafkaConsumerProgram extends Thread{  
  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("firstProducerProgram")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val topic1 = sparkStream(spark, "test_topic")
  val topic2 = sparkStream(spark, "test_topic")
  val topic3 = sparkStream(spark, "test_topic")
  val topic4 = sparkStream(spark, "test_topic")
  val topic5 = sparkStream(spark, "test_topic")
  val topic6 = sparkStream(spark, "test_topic")


  def sparkStream(spark: SparkSession, topic: String): DataFrame = {
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", topic)
      //.option("startingOffsets", "earliest")
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
    val df = topic1
      .selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")
      .select("key","value")
  }

  def q3(): Unit = {
  
  }

  def q4(): Unit = {
 
  }
}
