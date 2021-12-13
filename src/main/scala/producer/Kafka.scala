package producer

// Kafka imports
import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import producer.Api._

object Kafka {

  /**
    * Description: Object that handles the "producer" role of the project, but is itself not
    * instantiated in whole. Rather, classes are extended from its traits, and its methods are called
    * upon individually. Responsible for a consortium of responsibilities, including setup
    * of the Kafka producer and architecture by which messages are formatted, built, and sent.
    * @param None
    * @return None
  */


  // Declartion of properties and seupt for Producer side of Kafka. 

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
      "Recruiters"        *
      "Screeners"         *
      "Qualified_Lead"    * 
      "Contact_Attempts"  *
      "Screening"         * 
      "Offers"            * 
    *
    * 
    */
  sealed trait msgTypes {
    /**
      * Description: Trait used to instantiate individual classes representing the topics.
      * Methods include generating the data, formulating that data into a message, then sending
      * that message to a Kafka topic.
      * @param None
      * @return None
    */

    val topicName = "NaN"         // Topic name will be overridden when trait is instantiated.
    var id: Int = 0               // General identification for the person in question
    var msgCounter: Int = 0
    var msgData = Array[String]()

    def loadNewData(): Unit = ???
    /**
     * Description: Defines a blank function that will be overloaded for customization based on the
     * specific topic it applies to.
     * @param None
     * @return None
    */

    def messageGenerator(): String = {
    /**
     * Description: Default message creator that alters the ID number; will be 
     * overloaded based on the topic it is being applied to.
      * @param None
      * @return returnStr: modified string that will be sent as a message to Kafka.
    */
      if (this.msgCounter > this.msgData.length - 1) {
        this.loadNewData()
        this.msgCounter = 0
      }
      val returnStr = this.msgData(this.msgCounter)
        .replace("\"id\":1", "\"id\":" + this.id)
 
      returnStr
    }
 
    def sendMessage(): Unit = {
    /**
     * Description: Default function that is meant to send the message to Kafka. Will be overloaded
     * depending on the topic that uses this function.
     * @param None
     * @return None
    */
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
    /**
     * Description: Function that will generate a random number which determines how many messages of a 
     * specific topic are to be generated. 
     * @param None
     * @return None
    */      
      val rand = scala.util.Random;
      var next = rand.nextInt(100);
      if(next > 40)
        typeCounter += 1;
      if(typeCounter == 4)
        typeCounter = 0;
    }
 
  }

  // The next few case classes represent the topics that create classes via 
  // extending the msgTypes trait. The traits are overloaded based on which topic
  // is being used. 

  // First, new data is generated for the specific topic in hand. Then, the ID is set.

  case class Recruiters() extends msgTypes {

    this.msgData = recruiterData()

    override val topicName = "Recruiters"

    override def loadNewData(): Unit = {
      msgData = recruiterData()
    }

    def setID(): Unit = {
      val rand = scala.util.Random;
      this.id = rand.nextInt(100);
    }
  }
  
  case class Screeners() extends msgTypes {

    this.msgData = screenerData()

    override val topicName = "Screeners"

    override def loadNewData(): Unit = {
      msgData = screenerData()
    }
    def setID(): Unit = {
      val rand = scala.util.Random;
      this.id = rand.nextInt(100) + 100;
    }
  }

  case class QualifiedLead() extends msgTypes {

    this.msgData = qlData()

    override val topicName = "Qualified_Lead"

    override def loadNewData(): Unit = {
      msgData = qlData()
    }
  }

  case class ContactAttempts() extends msgTypes {

    this.msgData = caData()

    override val topicName = "Contact_Attempts"

    override def loadNewData(): Unit = {
      msgData = caData();
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

    this.msgData = screeningData()

    override val topicName = "Screening"

    override def loadNewData(): Unit = {
      msgData = screeningData()
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

    this.msgData = offerData()

    override val topicName = "Offers"

    override def loadNewData(): Unit = {
      msgData = offerData()
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

      extendOffer; // If the offer is not delayed, reset back to QL
      this.id += 1
      this.msgCounter += 1
      totalMsgCounter += 1
    }

    def extendOffer: Unit = { // extends offer is delayed/deffered
      var input = ((((msgData(this.msgCounter).split(","))(6)).split(":"))(1)).replace("\"", "");
      if(input != "Delay")
        typeCounter = 0;
    }

  }

  // These handlers instantiate new objects based on the (topic) class to be generated. The ID's are overridden
  // and a message is produced. The message is then sent to Kafka. 

  val recruitersHandler = new Recruiters()
  val qlHandler = new QualifiedLead()
  val caHandler = new ContactAttempts()
  val screeningHandler = new Screening()
  val offersHandler = new Offers()
  val screenersHandler = new Screeners()

  // These IDs are reset every time numMsg is met in the for-loop in msgStream. 

  recruitersHandler.id += 1
  screenersHandler.id += 101
  qlHandler.id += 200
 
 //Sends Data into the Recruiter and Screener Topics
  def msgStreamFirst(): Unit = {
    /**
     * Description: Function that sends Data into the Recruiter and Screener Topics
     * @param None
     * @return None
    */  
    for (i <- 0 until 100) {
      println("We are at: " + totalMsgCounter)
      recruitersHandler.sendMessage() 
      screenersHandler.sendMessage()
    }
  }
  
  def msgStream(numMsg: Int): Unit = {
    /**
     * Description: Function that will output a set of messages to each topic
     * @param numMsg: The he number of messages sent to the topics in total.
     * @return None
    */  
    def scrMsg: Unit = {
      /**
       * Description: Function that either sends a screening message or ends contact with current qualified lead.
       * @param None
       * @return None
      */  
      if((scala.util.Random).nextInt(10) > 1) { //80% chance a contact attempt will lead to a screening
          println("Sending screening data")
          screeningHandler.sendMessage(screenersHandler, qlHandler)
        }
        else
          typeCounter = 0;
    }

    // Iterate over the total number of messages that are to be sent (numMsg) to Kafka. Each iteration will send one
    // QL message, at least one CA message, one screening message, and at least one offers message.
    for (i <- 0 until numMsg) {
      println("We are at: " + totalMsgCounter)

      typeCounter match {
        case 0 => println("Sending ql data"); qlHandler.sendMessage()
        case 1 => println("Sending contact attempts data"); caHandler.sendMessage(recruitersHandler, qlHandler)
        case 2 => scrMsg
        case 3 => println("Sending offers data"); offersHandler.sendMessage(screenersHandler, recruitersHandler, qlHandler)
      }
    }
    Thread.sleep(2000) // Send message to Kafka once every two seconds
  }
}