package producer

// Project imports
import producer.Api._
import producer.Kafka._
import java.util.Properties

object mock extends App {

    /**
     * Description: Object that acts as "main" program that executes both the generation and deliver of mock data
     * to Kafka. Terminates when user provides "false" when prompted.
     * @param None
     * @return None
    */
    println("""██████╗ ███████╗██╗   ██╗ █████╗ ████████╗██╗   ██╗██████╗ ███████╗
██╔══██╗██╔════╝██║   ██║██╔══██╗╚══██╔══╝██║   ██║██╔══██╗██╔════╝
██████╔╝█████╗  ██║   ██║███████║   ██║   ██║   ██║██████╔╝█████╗  
██╔══██╗██╔══╝  ╚██╗ ██╔╝██╔══██║   ██║   ██║   ██║██╔══██╗██╔══╝  
██║  ██║███████╗ ╚████╔╝ ██║  ██║   ██║   ╚██████╔╝██║  ██║███████╗
╚═╝  ╚═╝╚══════╝  ╚═══╝  ╚═╝  ╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝
                                                                   """.stripMargin)
    Thread.sleep(1000)
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
    producer.close()

}
