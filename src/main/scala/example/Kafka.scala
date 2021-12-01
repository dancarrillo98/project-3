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


  
  /** ADT Sum Type Structure for each API call for each Topic
    * 
    * Companion Singleton Object to mainly keep track of ID's -
    * statically among all the objects
    *
    * sealed trait msgTypes 
    * case class Recruiters extends msgTypes 
    * case class QualifiedLead extends msgTypes 
    * case class ContactAttempts extends msgTypes
    * case class Screening extends msgTypes 
    * case class Offers extends msgTypes
    * case class Screeners extends msgTypes
    * 
    * 
    * Names for the Topics:
        "Recruiters"            
        "Qualified_Lead"        *
        "Contact_Attempts"      *
        "Screening"             *
        "Offers"                *
        "Screeners"             
    */
  sealed trait msgTypes {

    protected val topicName = "NaN"

    def loadNewData(): Unit = ???

    def messageGenerator(): String = ???

    def sendMessage(): Unit = {
      val msg = new ProducerRecord[String, String](
        this.topicName,
        Recruiters.totalMsgCounter.toString,
        this.messageGenerator()
      )

      producer.send(msg)

      Recruiters.totalMsgCounter += 1
    }

  }
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  case class Recruiters() extends msgTypes {

    var msgData = recruiterData()
    override val topicName = "Recruiters"

    override def loadNewData(): Unit = {
      msgData = recruiterData()
    }

    override def messageGenerator(): String = {
        if(Recruiters.msgCounter < msgData.length) {    //guards against out-of-bounds
            loadNewData()
            Recruiters.msgCounter = 0
        }
        val returnStr = msgData(Recruiters.msgCounter)
        .replace("\"id\":1", "\"id\":" + Recruiters.recruitersID)

        Recruiters.msgCounter += 1

        returnStr
    }

  }

  object Recruiters {
    var totalMsgCounter = 0 //Where to put this? Global scope?
    var recruitersID = 0
    var msgCounter = 0      //id's might serve the function of counters already ??
  }
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  case class QualifiedLead() extends msgTypes {

    var msgData = qlData()
    override val topicName = "Qualified_Lead"

    override def loadNewData(): Unit = {
      msgData = qlData()
    }

    override def messageGenerator(): String = {
        if(QualifiedLead.msgCounter < msgData.length){
            loadNewData()
            QualifiedLead.msgCounter = 0
        }
        val returnStr = msgData(QualifiedLead.msgCounter)
        .replace("\"id\":1", "\"id\":" + QualifiedLead.qlID)

      QualifiedLead.msgCounter += 1

      returnStr
    }

  }

  object QualifiedLead {
    var qlID = 0
    var msgCounter = 0
  }
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  case class ContactAttempts() extends msgTypes {

    var msgData = caData()
    override val topicName = "Contact_Attempts"

    override def loadNewData(): Unit = {
      msgData = caData()
    }

    override def messageGenerator(): String = {
        if(ContactAttempts.msgCounter < msgData.length) { 
            loadNewData()
            ContactAttempts.msgCounter = 0
        }
        val returnStr = msgData(ContactAttempts.msgCounter)
        .replace("\"ql_id\":1", "\"ql_id\":" + QualifiedLead.qlID)

      ContactAttempts.msgCounter += 1

      returnStr
    }

  }

  object ContactAttempts {
    var caID = 0
    var msgCounter = 0
  }
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  case class Screening() extends msgTypes {

    var msgData = screeningData()
    override val topicName = "Screening"

    override def loadNewData(): Unit = {
      msgData = screeningData()
    }

    override def messageGenerator(): String = {
        if(Screening.msgCounter < msgData.length) {
            loadNewData()
            Screening.msgCounter = 0
        }
        val returnStr = msgData(Screening.msgCounter)
        .replace("\"ql_id\":1", "\"ql_id\":" + QualifiedLead.qlID)

      Screening.msgCounter += 1

      returnStr
    }

  }

  object Screening {
    var screeningID = 0
    var msgCounter = 0
  }
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  case class Offers() extends msgTypes {

    var msgData = offerData()
    override val topicName = "Offers"

    override def loadNewData(): Unit = {
      msgData = offerData()
    }

    override def messageGenerator(): String = {
        if(Offers.msgCounter < msgData.length) {
            loadNewData()
            Offers.msgCounter = 0
        }
        val returnStr = msgData(Offers.msgCounter)
        .replace("\"ql_id\":1", "\"ql_id\":" + QualifiedLead.qlID)

      Offers.msgCounter += 1

      returnStr
    }

  }

  object Offers {
    var offerID = 0
    var msgCounter = 0
  }
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  case class Screeners() extends msgTypes {

    var msgData = screenerData()
    override val topicName = "Screeners"

    override def loadNewData(): Unit = {
      msgData = screenerData()
    }

    override def messageGenerator(): String = {
        if(Screeners.msgCounter < msgData.length)
            loadNewData()
            Screeners.msgCounter = 0

        val returnStr = msgData(Screeners.msgCounter)
        .replace("\"ql_id\":1", "\"ql_id\":" + QualifiedLead.qlID)

      Screeners.msgCounter += 1

      returnStr
    }

  }

  object Screeners {
    var screenerID = 0
    var msgCounter = 0
  }

  /** @func msgStream
    * Will output a set of messages to each topic
    * 
    * Gets the API data each time the function is called 
    * 
    * 
    * @param numMsg
    */
  def msgStream(numMsg: Int): Unit = {

    //val rand = scala.util.Random

    val qlHandler = new QualifiedLead()
    val caHandler = new ContactAttempts()
    val screeningHandler = new Screening()
    val offersHandler = new Offers()
    val screenersHandler = new Screeners()

    for (i <- 0 until numMsg) {
      println("We are at : " + Recruiters.totalMsgCounter)

      qlHandler.sendMessage()
      caHandler.sendMessage()
      screeningHandler.sendMessage()
      offersHandler.sendMessage()

      Thread.sleep(1000)

    }
  }
}












/** Shadow realm jimbo
  *
  * // def chanceNextMessage(typ: Int): Int = { // val rand = scala.util.Random
  * // val nextQ = rand.nextInt(100); // if(nextQ > 40) // return typ + 1; //
  * return typ // }
  * 
  * 
  * 
  * 
  * 
  * 
  * 
  * 
  * 
  * 
  * 
  * 
  */
