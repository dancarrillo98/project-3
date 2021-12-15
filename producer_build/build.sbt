import Dependencies._

ThisBuild / scalaVersion     := "2.11.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

// ThisBuild / scalacOptions ++= Seq("-unchecked", "-deprecation")

val allDeps = Seq(parseCombs, sparkCore, sparkSql, sparkSqlKafka, kafka, kafkaClients)

lazy val commonSettings = Seq(
  assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },
  libraryDependencies ++= allDeps
)

Global / excludeLintKeys += assemblyMergeStrategy

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    assembly / mainClass := Some("producer.mock"),
    assembly / assemblyJarName := "producer.jar",
    name := "project-3-producer"
  )

