import Dependencies._

ThisBuild / scalaVersion     := "2.11.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

ThisBuild / scalacOptions ++= Seq("-unchecked", "-deprecation")

val allDeps = Seq(scalaTest, parseCombs, sparkCore, sparkSql, sparkSqlKafka, kafka)

lazy val commonSettings = Seq(
  assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },
  libraryDependencies ++= allDeps
)

Global / excludeLintKeys += assemblyMergeStrategy

lazy val printConsumer_app = (project in file("."))
  .settings(
    commonSettings,
    assembly / mainClass := Some("consumer.PrintConsumer"),
    assembly / assemblyJarName := "PrintConsumer.jar",
    name := "project-3-PrintConsumer"
  )