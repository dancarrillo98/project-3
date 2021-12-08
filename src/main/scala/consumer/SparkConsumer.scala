package consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class SparkConsumer{

    def writeQualifiedLeadTotal(df: DataFrame): Unit = {
        //Obtain the value and key from the topic
        val dfSelect = df.select(col("key").cast("string"), col("value").cast("string"))
        println("Schema for DataFrame created from Qualified Lead Topic")
        dfSelect.printSchema()

       //Schema for Qualified Leads
       val qualifiedLeadSchema = new StructType()
        .add("id", IntegerType, false)
        .add("first_name", StringType, false)
        .add("last_name", StringType, false)
        .add("university", StringType, false)
        .add("major", StringType, false)
        .add("email", StringType, false)
        .add("home_state", StringType, false)

        //Apply schema to DF containing JSON data
       val qualifiedLeadDF = dfSelect.withColumn("JSON_data", from_json(col("value"), qualifiedLeadSchema)).select("JSON_data.*")
       println("Qualified Lead DataFrame Schema")
       qualifiedLeadDF.printSchema()

        //Write topic events to JSON file
       //writeDataFrameToFile(qualifiedLeadDF)

       println("Total Number of Qualified Leads")
       //Select total number of Qualified Leads
       val qualifiedLeadCount = qualifiedLeadDF.select(count("id") as "total_qualified_leads")
       
       //Output result to console
       val outputResult = qualifiedLeadCount.writeStream
       .outputMode("complete")
       .format("console")
       .start()
       
       //Stop streaming to console
       scala.io.StdIn.readLine("Showing Results\nPress Enter to Return to Main Menu\n")
       outputResult.stop()
    }

//SAMPLE OUTPUT
// root
//  |-- key: string (nullable = true)
//  |-- value: string (nullable = true)

// root
//  |-- id: integer (nullable = true)
//  |-- first_name: string (nullable = true)
//  |-- last_name: string (nullable = true)
//  |-- university: string (nullable = true)
//  |-- major: string (nullable = true)
//  |-- email: string (nullable = true)
//  |-- home_state: string (nullable = true)

// Total Number of Qualified Leads
// -------------------------------------------
// Batch: 0
// -------------------------------------------
// +---------------------+
// |total_qualified_leads|
// +---------------------+
// |                  156|
// +---------------------+

// -------------------------------------------
// Batch: 1
// -------------------------------------------
// +---------------------+
// |total_qualified_leads|
// +---------------------+
// |                  166|
// +---------------------+


    //Call this function using DF with schema as parameter to store events into JSON file
    def writeDataFrameToFile(df: DataFrame, filePath: String, checkPointPath: String): Unit = {
        //TODO
        //Change filename path after testing
        //Determine how to output a single file containing all results

        //Write DF to JSON file
        df.writeStream
        .outputMode("append")
        .format("json")
        .option("path", filePath)
        .option("checkpointLocation", checkPointPath)
        .start()
    }
}