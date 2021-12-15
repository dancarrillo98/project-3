import Dependencies._

ThisBuild / scalaVersion     := "2.11.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "mockaroo",
    libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3",  // 1.2.0 in examples
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3",   // 2.1.0 in examples
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.4",
    libraryDependencies += "org.apache.kafka" %% "kafka" % "2.1.0"
  )


//Testing Integration with Topic Code
// import Dependencies._

// ThisBuild / scalaVersion     := "2.11.8"
// ThisBuild / version          := "0.1.0-SNAPSHOT"
// ThisBuild / organization     := "com.example"
// ThisBuild / organizationName := "example"

// lazy val root = (project in file("."))
//   .settings(
//     assembly / mainClass := Some("example.TopicCreator"),
//     assembly / assemblyJarName := "kafka.jar",
//     name := "project-3",
//     libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
//     libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3",  // 1.2.0 in examples
//     libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3",   // 2.1.0 in examples
//     libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.4",
//     libraryDependencies += "org.apache.kafka" %% "kafka" % "2.1.0"

//   )

//   assemblyMergeStrategy in assembly := {
//     case PathList("META-INF", xs @ _*) => MergeStrategy.
//     discard
//     case x => MergeStrategy.first
//   }