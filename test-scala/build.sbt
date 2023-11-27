// Project settings
name := "WeatherStreamingDemo"
version := "1.0"
scalaVersion := "2.12.10"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.6",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.6",
  "org.apache.kafka" % "kafka-clients" % "2.0.0"
)
