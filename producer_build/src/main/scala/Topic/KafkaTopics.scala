package Topic

import scala.collection.JavaConverters._
import scala.util.control._
import scala.util.control.Breaks._
import scala.util.{Try, Success, Failure}
import java.util.{Collections, Properties}
import java.util.concurrent.TimeUnit
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ListTopicsOptions, NewTopic}
 import kafka.security.auth.Topic

object KafkaTopics {
  var adminClient: Option[AdminClient] = None

  var topics = Array("Qualified_Lead", "Contact_Attempts", "Screening", "Offers", "Recruiters", "Screeners")
  //init()
  //createTopics(topics)
  //listTopics().asScala.foreach(x => println(x))
  //cleanup()
  
  def init(host: String = "sandbox-hdp.hortonworks.com", port: Int = 6667): Unit = {
    if(adminClient.isEmpty) {
      val props: Properties = new Properties()

      props.put("bootstrap.servers", s"$host:$port")
      adminClient = Some(AdminClient.create(props))
      println("Connected to Kafka server as administrator")
      /*
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("acks", "all")
      */
    } else {
      println("Error: Already connected to Kafka")
    }
  }
  
  def cleanup() = {
    if(adminClient.isDefined) {
      adminClient.get.close()
    }
  }

  def listTopics(listInternal: Boolean = false, timeout: Int = 500): java.util.Set[String] = {
    if(adminClient.isDefined) {
      adminClient.get.listTopics((new ListTopicsOptions()).listInternal(listInternal).timeoutMs(timeout)).names.get
    } else {
      println("Error: Not connected to Kafka")
      new java.util.LinkedHashSet[String]()
    }
  }

  def topicExists(topic: String): Boolean = {
    if(adminClient.isDefined) {
      listTopics().contains(topic)
    } else {
      println("Error: Not connected to Kafka")
      false
    }
  }

  def createTopics(topics: Seq[String]): Unit = {
    topics.foreach(x => createTopic(x))
  }

  def createTopic(topic: String, partitions: Int = 1, replicationFactor: Short = 1): Boolean = {
    var result = false

    if(!topicExists(topic)) {
      val createTopicsResult = adminClient.get.createTopics(java.util.Collections.singletonList(new NewTopic(topic, partitions, replicationFactor)))

      var createTopicsResultIterator = createTopicsResult.values.entrySet.iterator
      while(createTopicsResultIterator.hasNext) {
        val entry = createTopicsResultIterator.next

        Try {
          //entry.getValue.thenApply((x) => println(x))
          println(s"${entry.getKey}: ${entry.getValue.get(2, TimeUnit.SECONDS)}")
        } match {
          case Success(_) => {}
          case Failure(exception) => println(exception)
        }
      }

      result = true
    } else {
      println(s"Error: Topic $topic already exists")
    }

    result
  }

  def deleteTopics(topics: Seq[String]): Unit = {
    topics.foreach(x => deleteTopic(x))
  }

  def deleteTopic(topic: String): Boolean = {
    var result = false

    if(topicExists(topic)) {
      val deleteTopicsResult = adminClient.get.deleteTopics(java.util.Collections.singletonList(topic))

      var deleteTopicsResultIterator = deleteTopicsResult.values.entrySet.iterator
      while(deleteTopicsResultIterator.hasNext) {
        val entry = deleteTopicsResultIterator.next

        Try {
          println(s"${entry.getKey}: ${entry.getValue.get(2, TimeUnit.SECONDS)}")
        } match {
          case Success(_) => {}
          case Failure(exception) => println(exception)
        }
      }

      result = true
    } else {
      println(s"Error: Topic '$topic' not found")
    }

    result
  }
}