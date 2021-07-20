package project.services
import org.apache.spark.sql.functions.{avg, col, from_json, struct, sum, to_json}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Encoders, SparkSession, functions}
import project.services.CalcAlerts.{DF, bootstrapServer, inputDF, schema, sparkSession, warmAlertsDF}
import project.services.CalcAlertsFromFileApp.{bootstrapServer, schema, sparkSession}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.serialization.StringDeserializer
import project.model.NormalizedStationCollectedData
import org.apache.spark.sql.functions.max

import java.util
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions.collectionAsScalaIterable
object temp extends App {
    val props:Properties = new Properties()
    props.put("group.id", "0")
    props.put("bootstrap.servers","cnt7-naya-cdh63:9092")//34.67.101.108:9092")
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")
   props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer(props)
    //consumer.subscribe(util.Arrays.asList("weather_info_verified_data"))
    val topic="weather_info_latest_raw_data"





  91043
 // val partitions = consumer.partitionsFor(topic).map(it=>new TopicPartition(topic,it.partition)).toList.asJavaCollection
 // consumer.assign(partitions)
 // consumer.seekToEnd(partitions)
  //val offsets = partitions.map(it => consumer.position(it)).toList.asJavaCollection
  //println("s")


  //91043

  val sparkSession: SparkSession = SparkSession.builder.master("local[*]").appName("example_app").getOrCreate
    val bootstrapServer="cnt7-naya-cdh63:9092"
    val schema = Encoders.product[NormalizedStationCollectedData].schema
    val a=sparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", "weather_info_verified_data")
      .option("startingOffsets", """{"weather_info_verified_data":{"0":91040}}""")
       .load
      . select(col("value").cast(StringType).alias("value"))

//    val DF = inputDF
//      .withColumn("parsed_json", from_json(col("value"), schema))
//      .select("parsed_json.*").groupBy(col("stationId")).agg(sum("value"))
//
//
//    println("e")
//    DF.select(to_json(struct("*")).as("value"))
//      .selectExpr("CAST(value AS STRING)")
//      .write
//      .format("kafka")
//      .option("kafka.bootstrap.servers", bootstrapServer)
//      .option("topic", "temp-dekel")
//      .save()
//
//    println("sss")


}
