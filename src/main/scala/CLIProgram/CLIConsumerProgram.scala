package CLIProgram

object CLIConsumerProgram {
  def main(args: Array[String]): Unit = {
    var loop = true
    val kc = new KafkaConsumerProgram()
   
    val thread = new Thread {
        override def run {
             kc.consumeFromKafka("TopicName") // Insert Topic Name
        }
    }

    val thread2 = new Thread {
        override def run {
             do{
                print("\u001b[2J")
                println("Please select an option")
                println("1. Question 1\n2. Question 2\n3. Question 3\n4. Question 4\n5. Quit Application")
                try {
                  
                  val option = scala.io.StdIn.readInt()
                  print("\u001b[2J")
                  option match{
                    case 1 => {
                      kc.q1()
                    }
                    case 2 => {
                      kc.q2()
                    }
                    case 3 => {
                    kc.q3()
                    }
                    case 4 => {
                      kc.q4()
                    }
                    case 5 => {
                      thread.stop()
                      loop = false
                    }
                }
                }catch {
                  case e: MatchError => println("Please pick a number between 0~5\n")
                  case e: NumberFormatException => println("\nPlease enter a number\n") 
                }
                
              } while(loop) 
      }
    }
    thread.start()
    Thread.sleep(10000)
    thread2.start()
  }
}