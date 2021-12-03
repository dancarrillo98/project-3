package producer

// Project imports
import producer.Api._
import producer.Kafka._
import java.util.Properties

object mock extends App {

    val rand = scala.util.Random;
    msgStreamFirst()

    var stop = false; // Testing
    while(!stop)
    {
        val msgNum = rand.nextInt(40) + 10; // Random number of msgs
        msgStream(msgNum);
        
        // Testing
        stop = scala.io.StdIn.readBoolean();        
    }
    producer.close()

}