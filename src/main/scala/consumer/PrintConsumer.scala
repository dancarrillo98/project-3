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
      while (true) {
        val records = consumer.poll(10)
        for (record <- records.asScala) {
          println("Topic: " + record.topic() + ", Key: " + record.key() + ", Offet: " + record.offset() + ", Partition: " + record.partition())
          println("Value: " + record.value())
          println("------------------------")
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      consumer.close()
    }
  }
}


        //createPrintConsumer() ->
        
    // Topic: Screeners
    // Value: {"id": 1,"first_name":"Alisa","last_name":"Figgures"}
    // ID: 1
    // First Name: Alisa
    // Last Name: Figgures
    // Topic: Screeners ID: 2, First Name: Oralle, Last Name: Druhan
    // Value: {"id": 2,"first_name":"Oralle","last_name":"Druhan"}
    // Topic: Recruiters
    // Value: {"id": 1,"first_name":"Brantley","last_name":"Pickance"}
    // Topic: Recruiters
    // Value: {"id": 2,"first_name":"Joyous","last_name":"Macconarchy"}
    // Topic: Qualified_Lead
    // Value: {"id": 1,"first_name":"Maddy","last_name":"Blees","university":"Fayetteville State University","major":"Business","email":"mblees0@wiley.com","NM"}
    // Topic: Contact_Attempts
    // Value: {"recruiter_id":3,"ql_id":1,"contact_date":"5/6/2018","start_time":"9:00:00 AM","end_time":"10:25:00 AM","contact_method":"Phone"}
    // Topic: Contact_Attempts
    // Value: {"screener_id":3,"ql_id":1,"screening_date":"6/17/2018","start_time":"10:00:00 AM","end_time":"11:26:00 AM","screening_type":"Spark","question_number":34,"question_accepted":14}
    // Topic: Offers
    // Value: {"screener_id":3,"recruiter_id":1,"ql_id":1,"offer_extended_date":"7/8/2018","offer_action_date":"7/15/2018","contact_method":"Email","offer_action":"Accept"}