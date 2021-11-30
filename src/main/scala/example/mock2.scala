package example


// Project imports
import example.Api._
import example.Kafka._

object mock extends App {

    loopingMsg(50)
    producer.close()

}