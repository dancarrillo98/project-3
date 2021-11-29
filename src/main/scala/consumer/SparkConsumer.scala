package consumer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SparkConsumer{
    def displayQualifiedLeadTotal(df: DataFrame): Unit = {
        val outputDF = df.select(count("id"))
        outputDF.show()
    }

    def writeDataFrameToFile(df: DataFrame, filePath: String): Unit = {
        df.write.mode("append").csv(filePath)
    }
}