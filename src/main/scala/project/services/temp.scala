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
import org.apache.spark.sql.functions.{max}
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._
object temp extends App {
    val props:Properties = new Properties()
   // props.put("group.id", "test")
    props.put("bootstrap.servers","34.67.101.108:9092")
    props.put("key.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer",
        "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")

    val consumer = new KafkaConsumer(props)
    println(consumer.listTopics())
    val topics = List("temp")
    consumer.subscribe(topics.asJava)
    val records = consumer.poll(1)




    val sparkSession: SparkSession = SparkSession.builder.master("local[*]").appName("example_app").getOrCreate
    val bootstrapServer="cnt7-naya-cdh63:9092"
    val schema = Encoders.product[NormalizedStationCollectedData].schema
    val a=sparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", "weather_info_verified_data").load
      .select(col("offset")).agg(max(col("offset")))
    println("kk")
   //   select(col("value").cast(StringType).alias("value"))

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
