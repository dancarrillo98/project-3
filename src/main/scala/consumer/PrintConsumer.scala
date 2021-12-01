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
      while (true) {
        val records = consumer.poll(10)
        for (record <- records.asScala) {
          println("Topic: " + record.topic())

          println(
            "Value: " + record.value()) + "\n\n"
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }
  }
}