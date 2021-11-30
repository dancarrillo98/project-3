package example

// Kafka imports
import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import example.Api._

object Kafka {

    // Declartion of properties
    val props: Properties = new Properties();
    
    props.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put("acks", "all")

    val producer = new KafkaProducer[String, String](props)


    
    sealed trait msgTypes {

        var msgType = ???

        def loadNewData(): Unit = ???

        def messageGenerator(): String = ???

        def sendMessage(handler: msgTypes): Unit = {
            val msg = new ProducerRecord[String, String] ( 
                  handler.topicName,
                  Recruiters.globalMsgCounter.toString,
                  handler.messageGenerator()
            )

            producer.send(msg)

            Recruiters.globalMsgCounter += 1 
        }
    }

    case class Recruiters() extends msgTypes {

        var msgArr = recruiterData()
        val topicName = "Recruiters"

        override def loadNewData(): Unit = {
            msgArr = recruiterData()
        }
        
// why are we += 1 on recruitersMsgCounter but using globalMsgCounter?
        override def messageGenerator(): String = {
            Recruiters.recruiterMsgCounter += 1
            msgArr(Recruiters.globalMsgCounter)
                .replace("\"id\":1","\"id\":" + Recruiters.recruiterID)
        }

    }

    //For static members
    object Recruiters {
        var recruiterID = 0
        var globalMsgCounter = 0
        var recruiterMsgCounter = 0
    }

    case class QualifiedLead() extends msgTypes {

        var msgArr = qlData()
        var topicName = "Qualified_Lead"
        var ql_id = 0

        override def loadNewData(): Unit = {
            msgArr = qlData()
        }

        override def messageGenerator(): String = {
            Recruiters.recruiterMsgCounter += 1
            msgArr(Recruiters.globalMsgCounter)
                .replace("\"id\":1","\"id\":" + Recruiters.recruiterID)
        }

    }

    case class ContactAttempts() extends msgTypes {

        var msgArr = caData()
        var topicName = "Contact_Attempts"

        override def loadNewData(): Unit = {
            msgArr = caData()
        }

        def chanceNextMessage(typ: Int): Int = {
            val rand = scala.util.Random
            val nextQ = rand.nextInt(100);
            if(nextQ > 40) 
                return typ + 1;
            return typ
        }

        override def messageGenerator(): String = {
            Recruiters.recruiterMsgCounter += 1
            msgArr(Recruiters.globalMsgCounter)
                .replace("\"ql_id\":1","\"ql_id\":" + Recruiters.recruiterID)
        }

    }

    object ContactAttempts {
        
    }

    case class Screening() extends msgTypes {

        var msgArr = screeningData()
        var topicName = "Screening"

        override def loadNewData(): Unit = {
            msgArr = screeningData()
        }

        override def messageGenerator(): String = {
            Recruiters.recruiterMsgCounter += 1
            msgArr(Recruiters.globalMsgCounter)
                .replace("\"ql_id\":1","\"ql_id\":" + Recruiters.recruiterID)
        }

    }

    case class Offers() extends msgTypes {

        var msgArr = offerData()
        var topicName = "Offers"

        override def loadNewData(): Unit = {
            msgArr = offerData()
        }

        override def messageGenerator(): String = {
            Recruiters.recruiterMsgCounter += 1
            msgArr(Recruiters.globalMsgCounter)
                .replace("\"ql_id\":1","\"ql_id\":" + Recruiters.recruiterID)
        }

    }

    case class Screeners() extends msgTypes {

        var msgArr = screenerData()
        var topicName = "Screeners"

        override def loadNewData(): Unit = {
            msgArr = screenerData()
        }

        override def messageGenerator(): String = {
            Recruiters.recruiterMsgCounter += 1
            msgArr(Recruiters.globalMsgCounter)
                .replace("\"ql_id\":1","\"ql_id\":" + Recruiters.recruiterID)
        }

    }
    

    def loopingMsg (numMsg: Int): Unit = {

            //val rand = scala.util.Random

            val qlHandler = new QualifiedLead()
            val caHandler = new ContactAttempts()
            val screeningHandler = new Screening()
            val offersHandler = new Offers()
            val screenersHandler = new Screeners()

            for(i <- 0 until numMsg) {
                println("We are at : " + Recruiters.globalMsgCounter)

                val msg = new ProducerRecord[String, String] ( 
                  qlHandler.topicName,
                  Recruiters.globalMsgCounter.toString,
                  qlHandler.messageGenerator()
                )

                val msg2 = new ProducerRecord[String, String] ( 
                  caHandler.topicName,
                  Recruiters.globalMsgCounter.toString,
                  caHandler.messageGenerator()
                )

                val msg3 = new ProducerRecord[String, String] ( 
                  offersHandler.topicName,
                  Recruiters.globalMsgCounter.toString,
                  offersHandler.messageGenerator()
                )

                val msg4 = new ProducerRecord[String, String] ( 
                  screenersHandler.topicName,
                  Recruiters.globalMsgCounter.toString,
                  screenersHandler.messageGenerator()
                )

                producer.send(msg)
                producer.send(msg2)
                producer.send(msg3)
                producer.send(msg4)

                Recruiters.globalMsgCounter += 1 

                Thread.sleep(2000)

            }
    }
}