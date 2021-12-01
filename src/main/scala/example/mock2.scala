package example

// Project imports
import example.Api._
import example.Kafka._

object mock extends App {

    msgStream(10)
    producer.close()

}