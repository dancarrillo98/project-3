import sbt._

object Dependencies {
  // lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9"
  lazy val parseCombs = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "2.4.3" // 1.2.0 in examples
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.4.3" // 2.1.0 in examples
  lazy val sparkSqlKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.4"
  lazy val kafka = "org.apache.kafka" %% "kafka" % "2.1.0"
  lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % "2.1.0"
}
