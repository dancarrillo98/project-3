package CLIProgram

import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._
import java.time.Duration


class  KafkaConsumerProgram extends Thread{
    def main(args: Array[String]): Unit = {
       
    }

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

    def q1(): Unit = {

    }
    def q2(): Unit = {

    }
    def q3(): Unit = {

    }
    def q4(): Unit = {

    }


}
