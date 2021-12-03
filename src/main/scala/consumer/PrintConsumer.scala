//Create a Producer for the Topic you want to write to
//$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list sandbox-hdp.hortonworks.com:6667 --topic Qualified_Lead
//Replace above topic name with Recruiters, Contact_Attempts, Screening, Offers, or Screeners

//REMEMBER TO CREATE THE TOPICS FIRST:
//$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic Qualified_Lead

//Run in maria_dev in another console to see output
//spark-submit --packages org.apache.spark:spark-sql-fka-0-10_2.11:2.3.0 --class consumer.PrintConsumer  project-3_2.11-1.0.jar

package consumer

import java.util.{Collections, Properties}
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

object PrintConsumer {
  def main(args: Array[String]): Unit = {
    createPrintConsumer()
  }

  def createPrintConsumer(): Unit = {

    //Define Properties
    val props: Properties = new Properties()
    props.put("group.id", "Revature")
    // props.put("bootstrap.servers","localhost:9092")
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

    //Define Topics
    val recruitersTopic = "Recruiters"
    val qualifiedLeadTopic = "Qualified_Lead"
    val contactAttemptsTopic = "Contact_Attempts"
    val screeningTopic = "Screening"
    val offerTopic = "Offers"
    val screenersTopic = "Screeners"

    //Create Consumer
    val consumer = new KafkaConsumer(props)
    val topics = List(recruitersTopic, qualifiedLeadTopic, contactAttemptsTopic, screeningTopic, offerTopic, screenersTopic)

    //Output data from Topics to console
    try {
      consumer.subscribe(topics.asJava)
      print("\u001b[2J")
      while (true) {
        val records = consumer.poll(10)
        for (record <- records.asScala) {
          println("\nTopic: " + record.topic() + ", Key: " + record.key() + ", Offet: " + record.offset() + ", Partition: " + record.partition())
          println("Value: " + record.value())
          println("----------------------------------------------------")
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }
  }
}
//SAMPLE OUTPUT   
// Topic: Screeners, Key: null, Offet: 25, Partition: 0
// Value: {"id": 2,"first_name":"Oralle","last_name":"Druhan"}
// ----------------------------------------------------

// Topic: Recruiters, Key: null, Offet: 19, Partition: 0
// Value:
// ----------------------------------------------------

// Topic: Recruiters, Key: null, Offet: 20, Partition: 0
// Value:
// ----------------------------------------------------

// Topic: Recruiters, Key: null, Offet: 21, Partition: 0
// Value: {"id": 1,"first_name":"Brantley","last_name":"Pickance"}
// ----------------------------------------------------

// Topic: Recruiters, Key: null, Offet: 22, Partition: 0
// Value: {"id": 2,"first_name":"Joyous","last_name":"Macconarchy"}
// ----------------------------------------------------
