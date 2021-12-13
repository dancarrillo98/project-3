package producer

// Project imports
import producer.Api._
import producer.Kafka._
import java.util.Properties

import Topic.KafkaTopics

object mock extends App {

    /**
     * Description: Object that acts as "main" program that executes both the generation and deliver of mock data
     * to Kafka. Terminates when user provides "false" when prompted.
     * @param None
     * @return None
    */

    KafkaTopics.init()
    KafkaTopics.createTopics(KafkaTopics.topics)
    
    val rand = scala.util.Random;
    msgStreamFirst() // This is for testing.

    var stop = false; // Testing
    while(!stop)
    {
        val msgNum = rand.nextInt(40) + 10; // Random number of msgs to be sent to Kafka.
        msgStream(msgNum);
        
        // Request user to continue or terminate program.
        println("Enter anthing to continue, or type 'false' to terminate producer.")
        //stop = scala.io.StdIn.readBoolean();        
        
    }

    KafkaTopics.cleanup()

    producer.close()


}
