

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object WeatherStreamingDemo {
  def main(args: Array[String]): Unit = {

    println("Weather Monitoring Streaming with Kafka demo started ...")

    val KAFKA_TOPIC_NAME_CONS = "weather"
    val KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val spark = SparkSession.builder.master("local[*]")
      .appName("Spark Structured Streaming with Kafka demo")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Stream from Kafka
    val weather_detail_df = spark.readStream
      .format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "latest")
      .load()

    println("Printing Schema of weather_detail_df: ")
    weather_detail_df.printSchema()

    val weather_detail_df_1 = weather_detail_df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

    // Define a schema for the transaction_detail data
    val transaction_detail_schema = StructType(Array(
      StructField("CityName", StringType),
      StructField("Temperature", DoubleType),
      StructField("Humidity", IntegerType),
      StructField("CreationTime", StringType)
    ))

    val weather_detail_df_2 = weather_detail_df_1.select(from_json(col("value"), transaction_detail_schema).as("weather_detail"), col("timestamp"))

    val weather_detail_df_3 = weather_detail_df_2.select("weather_detail.*", "timestamp")

    // Convert the CreationTime column to a date type before casting it to DateType
    val weather_detail_df_4 = weather_detail_df_3.withColumn("CreationDate", to_date(col("CreationTime").cast(TimestampType)))
    println("Printing Schema of weather_detail_df_4: ")
    weather_detail_df_4.printSchema()

    val weather_detail_df_5 = weather_detail_df_4.select("CityName", "Temperature", "Humidity", "CreationDate")
    println("Printing Schema of weather_detail_df_5: ")
    weather_detail_df_5.printSchema()

    // Write final result into the console for debugging purposes
    val weather_detail_write_stream = weather_detail_df_5
      .writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("append")
      .format("console")
      .start()

    // Write the final result to HDFS
    val query = weather_detail_df_5.writeStream
      .format("csv")
      .option("path", "data/weather_detail")
      .option("checkpointLocation", "data/weather_detail_checkpoint")
      .start()

    query.awaitTermination()
    println("Weather Monitoring Streaming with Kafka demo Completed")
  }
}
