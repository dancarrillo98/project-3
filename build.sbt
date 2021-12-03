
ThisBuild / scalaVersion     := "2.11.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    assembly / mainClass := Some("Topic.KafkaTopics"),
    assembly / assemblyJarName := "kafka.jar",
    name := "scalastreaming",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0",
    //,libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
    //,libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
    //,libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
    // For kafka.
    libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.4",
    libraryDependencies += "org.apache.kafka" %% "kafka" % "2.1.0"
    /*libraryDependencies ++= Seq(
        "com.101tec" % "zkclient" % "0.4",
        "org.apache.kafka" % "kafka_2.10" % "0.8.1.1"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri"))*/
  )
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.
    discard
    case x => MergeStrategy.first
  }
// Uncomment the following for publishing to Sonatype.
// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for more detail.
// ThisBuild / description := "Some descripiton about your project."
// ThisBuild / licenses    := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
// ThisBuild / homepage    := Some(url("https://github.com/example/project"))
// ThisBuild / scmInfo := Some(
//   ScmInfo(
//     url("https://github.com/your-account/your-project"),
//     "scm:git@github.com:your-account/your-project.git"
//   )
// )
// ThisBuild / developers := List(
//   Developer(
//     id    = "Your identifier",
//     name  = "Your Name",
//     email = "your@email",
//     url   = url("http://your.url")
//   )
// )
// ThisBuild / pomIncludeRepository := { _ => false }
// ThisBuild / publishTo := {
//   val nexus = "https://oss.sonatype.org/"
//   if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
//   else Some("releases" at nexus + "service/local/staging/deploy/maven2")
// }
// ThisBuild / publishMavenStyle := true