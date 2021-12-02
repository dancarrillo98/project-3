import consumer.PrintConsumer.createPrintConsumer
import consumer.SparkConsumer.writeQualifiedLeadTotal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset

object Main  {
    def main(args: Array[String]): Unit = {

        //createPrintConsumer()
        
    // Topic: Screeners
    // Value: {"id": 1,"first_name":"Alisa","last_name":"Figgures"}
    // ID: 1
    // First Name: Alisa
    // Last Name: Figgures
    // Topic: Screeners ID: 2, First Name: Oralle, Last Name: Druhan
    // Value: {"id": 2,"first_name":"Oralle","last_name":"Druhan"}
    // Topic: Recruiters
    // Value: {"id": 1,"first_name":"Brantley","last_name":"Pickance"}
    // Topic: Recruiters
    // Value: {"id": 2,"first_name":"Joyous","last_name":"Macconarchy"}
    // Topic: Qualified_Lead
    // Value: {"id": 1,"first_name":"Maddy","last_name":"Blees","university":"Fayetteville State University","major":"Business","email":"mblees0@wiley.com","NM"}
    // Topic: Contact_Attempts
    // Value: {"recruiter_id":3,"ql_id":1,"contact_date":"5/6/2018","start_time":"9:00:00 AM","end_time":"10:25:00 AM","contact_method":"Phone"}
    // Topic: Contact_Attempts
    // Value: {"screener_id":3,"ql_id":1,"screening_date":"6/17/2018","start_time":"10:00:00 AM","end_time":"11:26:00 AM","screening_type":"Spark","question_number":34,"question_accepted":14}
    // Topic: Offers
    // Value: {"screener_id":3,"recruiter_id":1,"ql_id":1,"offer_extended_date":"7/8/2018","offer_action_date":"7/15/2018","contact_method":"Email","offer_action":"Accept"}

    }
}