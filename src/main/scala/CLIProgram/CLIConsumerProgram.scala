package CLIProgram

object CLIConsumerProgram {
  def main(args: Array[String]): Unit = {
    var loop = true
    val kc = new KafkaConsumerProgram()

    val thread2 = new Thread {
        override def run {
             kc.writeTopicsToFile()
             do{
                print("\u001b[2J")
                println("Please select information to display:")
                println("1. Total Number of Qualified Leads" +
                      "\n2. Total Number of Contact Attempts and Attempts per Recruiter" +
                      "\n3. Total Number of Screenings and Attempts per Screener" +
                      "\n4. Total Number of Offers and Offers by Action" +
                      "\n5. Quit Application")
                try {
                  
                  val option = scala.io.StdIn.readInt()
                  //print("\u001b[2J")
                  println()
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
                      kc.mergeFiles()
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
    thread2.start()
  }
}