import consumer.PrintConsumer.printTopicData
import consumer.SparkConsumer.displayQualifiedLeadTotal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Dataset

object Main  {
    def main(args: Array[String]): Unit = {
            val spark = SparkSession.builder
            .appName("Project 3")
            .config("spark.master", "local[*]")
            .getOrCreate()

        import spark.implicits._

        spark.sparkContext.setLogLevel("ERROR")

        //Qualified Lead Schema
        val qualifiedLeadSchema = StructType(
            List(
                StructField("id", IntegerType, true), 
                StructField("first_name", StringType, true), 
                StructField("last_name", StringType, true), 
                StructField("university", StringType, true), 
                StructField("major", StringType, true), 
                StructField("email", StringType, true), 
                StructField("home_state", StringType, true)
                )
            )

        //Read contents of CSV file from Topic after putting it in folder
        val csvDF = spark.readStream.format("csv")
            .option("maxFilesPerTrigger", 2)
            .option("header", true)
            .option("path", "data/stream")
            .schema(qualifiedLeadSchema)
            .load()
        println("CSV File Schema")
        csvDF.printSchema()
        
        //Format DataFrame so that the schema is a key-value pair
        val csvFormattedDF = csvDF.withColumn("value", concat_ws("-", col("first_name"), col("last_name"), col("university"), col("major"), col("email"), col("home_state")))
            .withColumn("key", col("id"))
            .drop("id")
            .drop("first_name")
            .drop("last_name")
            .drop("university")
            .drop("major")
            .drop("email")
            .drop("home_state")
        println("Formatted Schema")
        csvFormattedDF.printSchema()

        //Delete checkpoint files each time for testing.
        //Write CSV file into Topic
        csvFormattedDF.selectExpr("CAST (key AS STRING) AS key", "CAST(value AS STRING) AS value")
            .writeStream.format("kafka")
            .outputMode("append")
            .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
            .option("topic", "test_topic")
            .option("checkpointLocation", "file:///home/maria_dev/output/checkpoint/filesink_checkpoint")
            .start()


        val inputDF = spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
            .option("subscribe", "test_topic")
            .load()
            //.select(col("value").cast("string"))
            .select(col("key").cast("string"), col("value").cast("string"))

        println("DF From Topic")
        inputDF.printSchema()

        val outputDF = inputDF.withColumn("id", col("key"))
            .withColumn("first_name", split(col("value"), "-").getItem(0))
            .withColumn("last_name", split(col("value"), "-").getItem(1))
            .withColumn("university", split(col("value"), "-").getItem(2))
            .withColumn("major", split(col("value"), "-").getItem(3))
            .withColumn("email", split(col("value"), "-").getItem(4))
            .withColumn("home_state", split(col("value"), "-").getItem(5))
            .drop("key")
            .drop("value")

        println("Formatted DF For Output")
        outputDF.printSchema()
        //outputDF.show(false)

        printTopicData(outputDF)
        spark.streams.awaitAnyTermination()

//SAMPLE OUTPUT
//         -------------------------------------------
// Batch: 0
// -------------------------------------------
// +---+----------+---------+----------+-----+-----+----------+
// |id |first_name|last_name|university|major|email|home_state|
// +---+----------+---------+----------+-----+-----+----------+
// +---+----------+---------+----------+-----+-----+----------+

// -------------------------------------------
// Batch: 1
// -------------------------------------------
// +---+----------+----------+----------------------------------------------------------------------------------------------------+------------------------+-----------------------------+--------------------------+
// |id |first_name|last_name |university                                                                                          |major                   |email                        |home_state                |
// +---+----------+----------+----------------------------------------------------------------------------------------------------+------------------------+-----------------------------+--------------------------+
// |1  |Humberto  |Penny     |University Institute of Modern Languages                                                            |Research and Development|hpenny0@newyorker.com        |FL                        |
// |2  |Maryjo    |Claiden   |Yaroslavl International University of Business and New Technologies                                 |Services                |mclaiden1@yelp.com           |TX                        |
// |3  |Gisela    |Michelotti|Faculdades Integradas Toledo                                                                        |Sales                   |gmichelotti2@discovery.com   |TX                        |
// |4  |Ezekiel   |Bellis    |NTB Interstate University of Applied Sciences of Technology                                         |Engineering             |ebellis3@unicef.org          |NY                        |
// |5  |Ivan      |Crouse    |Université de Kolwezi                                                                               |Marketing               |icrouse4@yahoo.co.jp         |AZ                        |
// |6  |Kaylee    |Hinchon   |St. Petersburg State University of Civil Aviation                                                   |Sales                   |khinchon5@youku.com          |NY                        |
// |7  |Michell   |Scherme   |ARYA Institute of Engineering & Technology                                                          |Legal                   |mscherme6@engadget.com       |IN                        |
// |8  |Ebba      |Griswood  |Pennsylvania State University at Erie                                                               | Behrend College        |Accounting                   |egriswood7@dedecms.com    |
// |9  |Shandra   |Dresse    |Dijla University College                                                                            |Business Development    |sdresse8@a8.net              |CA                        |
// |10 |Rex       |Imison    |Universidad de Puerto Rico, Aguadilla                                                               |Services                |rimison9@techcrunch.com      |KY                        |
// |11 |Brigitte  |Swindon   |Universidad Nacional de Cuyo                                                                        |Business Development    |bswindona@accuweather.com    |NJ                        |
// |12 |Howey     |Chase     |Ecole Nationale d'Ingénieurs des Travaux Agricoles de Bordeaux                                      |Business Development    |hchaseb@time.com             |MN                        |
// |13 |Ignacius  |Courtois  |Xi'an Jiaotong                                                                                      |Liverpool University    |Human Resources              |icourtoisc@marketwatch.com|
// |14 |Ofelia    |Rollings  |Buraydah College for Applied Medical Sciences                                                       |Sales                   |orollingsd@statcounter.com   |TN                        |
// |15 |Thane     |Devall    |Universität Dortmund                                                                                |Engineering             |tdevalle@sohu.com            |NY                        |
// |16 |Jill      |Hardie    |Wilberforce University                                                                              |Product Management      |jhardief@un.org              |KY                        |
// |17 |Demetra   |Goodie    |Evangelische Fachhochschule Freiburg, Hochschule für Soziale Arbeit, Diakonie und Religionspädagogik|Engineering             |dgoodieg@guardian.co.uk      |IN                        |
// |18 |Reinhold  |Marzello  |Tabriz University                                                                                   |Support                 |rmarzelloh@chicagotribune.com|HI                        |
// |19 |Francyne  |Adenet    |University of Szczecin                                                                              |Marketing               |fadeneti@toplist.cz          |GA                        |
// |20 |Nicki     |Dutnell   |Alpha Omega University                                                                              |Support                 |ndutnellj@ibm.com            |FL                        |
// +---+----------+----------+----------------------------------------------------------------------------------------------------+------------------------+-----------------------------+--------------------------+
// only showing top 20 rows

// -------------------------------------------
// Batch: 2
// -------------------------------------------
// +---+-----------+-----------+---------------------------------------------------------+------------------------+-----------------------------+----------+
// |id |first_name |last_name  |university                                               |major                   |email                        |home_state|
// +---+-----------+-----------+---------------------------------------------------------+------------------------+-----------------------------+----------+
// |68 |Tildy      |Pottinger  |Université Victor Segalen (Bordeaux II)                  |Business Development    |tpottinger1v@nifty.com       |OH        |
// |69 |Clarita    |Stockle    |Université de Ziguinchor                                 |Business Development    |cstockle1w@ted.com           |CT        |
// |70 |Gabe       |Radford    |Ecole Supérieure des Sciences Economiques et Commerciales|Services                |gradford1x@wp.com            |CA        |
// |71 |Annecorinne|Cowle      |Alsadrain University                                     |Sales                   |acowle1y@hud.gov             |NY        |
// |72 |Reider     |Barfitt    |Lander University                                        |Engineering             |rbarfitt1z@state.tx.us       |GA        |
// |73 |Cecily     |Yorke      |Universidade do Sul de Santa Catarina                    |Research and Development|cyorke20@booking.com         |OR        |
// |74 |Jeremie    |Rickardsson|Universitat Jaume I de Castellón                         |Marketing               |jrickardsson21@guardian.co.uk|DC        |
// |75 |Gerhard    |Baxandall  |Symbiosis International University                       |Research and Development|gbaxandall22@tripadvisor.com |MI        |
// |76 |Brendis    |Castro     |Singapore Management University                          |Accounting              |bcastro23@skype.com          |CA        |
// |77 |Morgen     |Darnbrough |Northwest College of Art                                 |Product Management      |mdarnbrough24@is.gd          |WA        |
// |78 |Iolande    |Thewys     |Montserrat College of Art                                |Human Resources         |ithewys25@ucoz.com           |IL        |
// |79 |Jaquith    |Tawse      |Instituto Politécnico de Santarém                        |Support                 |jtawse26@washington.edu      |PA        |
// |80 |Ashby      |Guerro     |St. John's University                                    |Research and Development|aguerro27@deviantart.com     |TX        |
// |81 |Katuscha   |Legan      |Sebatian Kolowa University College                       |Business Development    |klegan28@google.ca           |NJ        |
// |82 |Alicea     |Lorenc     |Yamagata University                                      |Engineering             |alorenc29@google.com.hk      |IN        |
// |83 |Dannie     |Boggon     |Istanbul Bilgi University                                |Support                 |dboggon2a@wix.com            |MN        |
// |84 |Lettie     |Huglin     |Institut des Sciences de l'Ingénieur de Montpellier      |Support                 |lhuglin2b@un.org             |TX        |
// |85 |Katey      |Gilloran   |Hormozgan University of Medical Sciences                 |Marketing               |kgilloran2c@nydailynews.com  |WV        |
// |86 |Kaylee     |Ludron     |Yaroslavl State University                               |Support                 |kludron2d@wufoo.com          |NY        |
// |87 |Shirl      |Vasiltsov  |Cumhuriyet (Republik) University                         |Marketing               |svasiltsov2e@ed.gov          |CA        |
// +---+-----------+-----------+---------------------------------------------------------+------------------------+-----------------------------+----------+
// only showing top 20 rows

    }
}