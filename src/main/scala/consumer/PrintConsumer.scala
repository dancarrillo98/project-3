package consumer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object PrintConsumer{
    def printTopicData(df: DataFrame): Unit = {
        df.writeStream
          .outputMode("update")
          .format("console")
          .option("truncate", false)
          .start()
    }
}