package example

// Kafka imports
import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import example.Api._

object Kafka {

  // Declartion of properties
  val prodProps: Properties = new Properties()

  prodProps.put("bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
  prodProps.put(
    "key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  prodProps.put(
    "value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  prodProps.put("acks", "all")

  val producer = new KafkaProducer[String, String](prodProps)

  var totalMsgCounter = 0

  /** ADT Sum Type Structure for each API call for each Topic
    *
    *
    * sealed trait msgTypes 
      case class Recruiters extends msgTypes 
      case class Screeners extends msgTypes 
      case class QualifiedLead extends msgTypes 
      case class ContactAttempts extends msgTypes 
      case class Screening extends msgTypes 
      case class Offers extends msgTypes
    *
    * Names for the Topics: 
      "Recruiters" 
      "Screeners"
      "Qualified_Lead"    * 
      "Contact_Attempts"  *
      "Screening"         * 
      "Offers"            * 
    *
    * 
    */
  sealed trait msgTypes {

    protected val topicName = "NaN"
    protected var id: Int = 0
    protected var msgCounter: Int = 0
    protected var msgData: Array[String] = Array[String]()

    def loadNewData(): Unit = ???

    def messageGenerator(): String = {
      if (this.msgCounter > this.msgData.length - 1) {
        this.loadNewData()
        this.msgCounter = 0
      }
      val returnStr = this.msgData(this.msgCounter)
        .replace("\"id\":1", "\"id\":" + this.id)

      msgCounter += 1

      returnStr
    }

    def messageGenerator(qlHand: QualifiedLead): String = {
      if (this.msgCounter > this.msgData.length - 1) {
        this.loadNewData()
        this.msgCounter = 0
      }
      val returnStr = this.msgData(this.msgCounter)
        //.replace("\"id\":1", "\"id\":" + this.id)
        .replace("\"ql_id\":1", "\"ql_id\":" + qlHand.id)

      this.msgCounter += 1

      returnStr
    }

    def sendMessage(): Unit = {
      val msg = new ProducerRecord[String, String](
        this.topicName,
        totalMsgCounter.toString,
        this.messageGenerator
      )

      val meta = producer.send(msg);

      totalMsgCounter += 1
    }

    def sendMessage(qlHand: QualifiedLead): Unit = {
      val msg = new ProducerRecord[String, String](
        this.topicName,
        totalMsgCounter.toString,
        this.messageGenerator(qlHand)
      )

      val meta = producer.send(msg);

      totalMsgCounter += 1
    }

  }



  case class Recruiters() extends msgTypes {

    override val topicName = "Recruiters"

    override def loadNewData(): Unit = {
      msgData = recruiterData()
    }
  }
  
  case class Screeners() extends msgTypes {

    override val topicName = "Screeners"

    override def loadNewData(): Unit = {
      msgData = screenerData()
    }

    override def messageGenerator(): String = {
      if (msgCounter > msgData.length) {
        loadNewData()
        msgCounter = 0
      }
      val returnStr = msgData(msgCounter)

      msgCounter += 1

      returnStr
    }

  }

  case class QualifiedLead() extends msgTypes {

    override val topicName = "Qualified_Lead"

    override def loadNewData(): Unit = {
      msgData = qlData()
    }

  }

  case class ContactAttempts() extends msgTypes {

    override val topicName = "Contact_Attempts"

    override def loadNewData(): Unit = {
      msgData = caData()
    }

  }

  case class Screening() extends msgTypes {

    override val topicName = "Screening"

    override def loadNewData(): Unit = {
      msgData = screeningData()
    }

  }

  case class Offers() extends msgTypes {

    override val topicName = "Offers"

    override def loadNewData(): Unit = {
      msgData = offerData()
    }

  }



  val qlHandler = new QualifiedLead()
  val caHandler = new ContactAttempts()
  val screeningHandler = new Screening()
  val offersHandler = new Offers()
  val screenersHandler = new Screeners()


  qlHandler.loadNewData()
  caHandler.loadNewData()
  screeningHandler.loadNewData()
  offersHandler.loadNewData()
  screenersHandler.loadNewData()

  /** msgStream Will output a set of messages to each topic
    * 
    *
    * case class have guards against running out of data
    *
    * @param numMsg = the number of messages sent to the topics in total
    */
  def msgStream(numMsg: Int): Unit = {

    for (i <- 0 until numMsg) {
      println("We are at : " + totalMsgCounter)

      qlHandler.sendMessage()
      caHandler.sendMessage()

      screeningHandler.sendMessage()
      offersHandler.sendMessage()
      Thread.sleep(2000)
    }
  }
}
