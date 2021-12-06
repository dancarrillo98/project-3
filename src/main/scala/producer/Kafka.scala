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

  var totalMsgCounter = 0;
  var typeCounter = 0;

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

    val topicName = "NaN"
    var id: Int = 0
    var msgCounter: Int = 0
    var msgData: Array[String] = Array[String]()

    def loadNewData(): Unit = ???

    def messageGenerator(): String = {
      if (this.msgCounter > this.msgData.length - 1) {    // < BAD > GOOD 
        this.loadNewData()
        this.msgCounter = 0
      }
      val returnStr = this.msgData(this.msgCounter)
        .replace("\"id\":1", "\"id\":" + this.id)

      returnStr
    }
 
    def sendMessage(): Unit = {
      this.id += 1
      val msg = new ProducerRecord[String, String](
        this.topicName,
        totalMsgCounter.toString,
        this.messageGenerator
      )

      val meta = producer.send(msg);


      this.msgCounter += 1
      totalMsgCounter += 1
      if(this.topicName == "Qualified_Lead") {
        typeCounter += 1; // Always lead to contact attempt
        recruitersHandler.setID();
        screenersHandler.setID();
      }
    }
    def chance(): Unit = { // Decides to go to the next stage, later will decide to end completely
      val rand = scala.util.Random;
      var next = rand.nextInt(100);
      if(next > 40)
        typeCounter += 1;
      if(typeCounter == 4)
        typeCounter = 0;
    }
 
  }



  case class Recruiters() extends msgTypes {

    override val topicName = "Recruiters"

    override def loadNewData(): Unit = {
      msgData = recruiterData()
      Thread.sleep(5000)
    }

    def setID(): Unit = {
      val rand = scala.util.Random;
      this.id = rand.nextInt(100);
    }
  }
  
  case class Screeners() extends msgTypes {

    override val topicName = "Screeners"

    override def loadNewData(): Unit = {
      msgData = screenerData()
      Thread.sleep(5000)
    }
    def setID(): Unit = {
      val rand = scala.util.Random;
      this.id = rand.nextInt(100) + 100;
    }
  }

  case class QualifiedLead() extends msgTypes {

    override val topicName = "Qualified_Lead"

    override def loadNewData(): Unit = {
      msgData = qlData()
      Thread.sleep(5000) 
    }
  }

  case class ContactAttempts() extends msgTypes {

    override val topicName = "Contact_Attempts"

    override def loadNewData(): Unit = {
      msgData = caData()
      Thread.sleep(5000)
    }

    def messageGenerator(recruiterH: Recruiters,
                         qlH: QualifiedLead): String = {
      if (this.msgCounter > this.msgData.length - 1) {
        this.loadNewData()
        this.msgCounter = 0
      }
      val returnStr = this.msgData(this.msgCounter)
        .replace("\"recruiter_id\":1", "\"recruiter_id\":" + recruiterH.id)
        .replace("\"ql_id\":1", "\"ql_id\":" + qlH.id)

      returnStr
    }

    def sendMessage(recruiterH: Recruiters,
                    qlH: QualifiedLead): Unit = {
      val msg = new ProducerRecord[String, String](
        this.topicName,
        totalMsgCounter.toString,
        this.messageGenerator(recruiterH, qlH)
      )

      val meta = producer.send(msg);

      this.id += 1
      this.msgCounter += 1
      totalMsgCounter += 1
      chance();
    }
  }

  case class Screening() extends msgTypes {

    override val topicName = "Screening"

    override def loadNewData(): Unit = {
      msgData = screeningData()
      Thread.sleep(5000)
    }
     
    def messageGenerator(screenerH: Screeners,
                         qlH: QualifiedLead): String = {
      if (this.msgCounter > this.msgData.length - 1) {
        this.loadNewData()
        this.msgCounter = 0
      }
      val returnStr = this.msgData(this.msgCounter)
        .replace("\"screener_id\":1", "\"screener_id\":" + screenerH.id)
        .replace("\"ql_id\":1", "\"ql_id\":" + qlH.id)

      returnStr
    }

    def sendMessage(screenerH: Screeners,
                    qlH: QualifiedLead): Unit = {
      val msg = new ProducerRecord[String, String](
        this.topicName,
        totalMsgCounter.toString,
        this.messageGenerator(screenerH, qlH)
      )

      val meta = producer.send(msg);

      this.id += 1
      this.msgCounter += 1
      totalMsgCounter += 1
      chance();
    }
  }

  case class Offers() extends msgTypes {

    override val topicName = "Offers"

    override def loadNewData(): Unit = {
      msgData = offerData()
      Thread.sleep(5000)
    }

    def messageGenerator(screenerH: Screeners,
                         recruiterH: Recruiters,
                         qlH: QualifiedLead): String = {
      if (this.msgCounter > this.msgData.length - 1) {
        this.loadNewData()
        this.msgCounter = 0
      }
      val returnStr = this.msgData(this.msgCounter)
        .replace("\"screener_id\":1", "\"screener_id\":" + screenerH.id)
        .replace("\"recruiter_id\":1", "\"recruiter_id\":" + recruiterH.id)
        .replace("\"ql_id\":1", "\"ql_id\":" + qlH.id)

      returnStr
    }

    def sendMessage(screenerH: Screeners,
                    recruiterH: Recruiters,
                    qlH: QualifiedLead): Unit = {
      val msg = new ProducerRecord[String, String](
        this.topicName,
        totalMsgCounter.toString,
        this.messageGenerator(screenerH, recruiterH, qlH)
      )

      val meta = producer.send(msg);

      this.id += 1
      this.msgCounter += 1
      totalMsgCounter += 1
      chance();
    }

  }


  val recruitersHandler = new Recruiters()
  val qlHandler = new QualifiedLead()
  val caHandler = new ContactAttempts()
  val screeningHandler = new Screening()
  val offersHandler = new Offers()
  val screenersHandler = new Screeners()

  recruitersHandler.loadNewData()
  screenersHandler.loadNewData()
  qlHandler.loadNewData()
  caHandler.loadNewData()
  screeningHandler.loadNewData()
  offersHandler.loadNewData()

  recruitersHandler.id += 1
  screenersHandler.id += 101
  qlHandler.id += 200

  /** msgStream Will output a set of messages to each topic
    * 
    *
    * case class have guards against running out of data
    *
    * @param numMsg = the number of messages sent to the topics in total
    */
 
 //Sends Data into the Recruiter and Screener Topics
  def msgStreamFirst(): Unit = {
    
    for (i <- 0 until 100) {
      println("We are at: " + totalMsgCounter)

      recruitersHandler.sendMessage() 
      screenersHandler.sendMessage()
    }
  }
 
  def msgStream(numMsg: Int): Unit = {

    for (i <- 0 until numMsg) {
      println("We are at: " + totalMsgCounter)
      if(typeCounter == 0)
        qlHandler.sendMessage()
      else if(typeCounter == 1)
        caHandler.sendMessage(recruitersHandler, qlHandler)
      else if(typeCounter == 2)
        screeningHandler.sendMessage(screenersHandler, qlHandler)
      else if(typeCounter == 3)
        offersHandler.sendMessage(screenersHandler, recruitersHandler, qlHandler)
    }
    Thread.sleep(2000)
  }
}

















/** Shadow realm jimbo
  *
  * // def chanceNextMessage(typ: Int): Int = { // val rand = scala.util.Random
  * // val nextQ = rand.nextInt(100); // if(nextQ > 40) // return typ + 1; //
  * return typ // }
  */