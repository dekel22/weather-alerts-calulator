package project.services
import project.model.{Farm, NormalizedStationCollectedData}
import com.sun.org.slf4j.internal.LoggerFactory
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{avg, col, element_at, from_json, lit, struct, sum, to_json}
import org.apache.spark.sql.types.StringType

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object CalcAlerts extends App {
  val bootstrapServer = "https://35.225.147.228:9092";
  val sparkSession: SparkSession = SparkSession.builder.master("local[*]").appName("example_app").getOrCreate
  val farmSchema = Encoders.product[Farm].schema
  val farms = sparkSession.read.schema(farmSchema).json("farms\\*")



  val schema = Encoders.product[NormalizedStationCollectedData].schema
  val inputDF = sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServer)
    .option("subscribe", "weather_info_verified_data")
    .load
    //.filter last3 hours
    .select(col("value").cast(StringType).alias("value"))


    val DF = inputDF
    .withColumn("parsed_json", from_json(col("value"), schema))
    .select("parsed_json.*")

  //aggregate weather measurement and extend it to growing fields
  val aggDF=DF.groupBy(col("stationId"),col("channel")).agg(avg(col("value")),sum("value"))
  val heatDF=DF.filter(col("channel")==="TG").select(col("stationId"),col("value").as("warm_level"))
  val windDF= DF.filter(col("channel")==="WS").select(col("stationId"),col("value").as("wind_level"))
  val fieldsExtended=heatDF.join(windDF,"stationId").join(farms,"stationId")
  val DfWithDry= fieldsExtended.withColumn("dry_level",col("warm_level")+col("wind_level"))


  val DfWithHandle=DfWithDry.withColumn("dry_handle",col("dry_sensitivity")-col("dry_level")).
    withColumn("warm_handle",col("warm_sensitivity")-col("warm_level"))
  val warmAlertsDF=DfWithHandle.filter(col("warm_handle")<0).select(col("email"),col("name"),col("crops"))
  val dryAlertsDF=DfWithHandle.filter(col("dry_handle")<0).select(col("email"),col("name"),col("crops"))

  warmAlertsDF.select(to_json(struct("*")).as("value"))
    .selectExpr("CAST(value AS STRING)")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServer)
    .option("topic", "dry_alerts")
    .save()

  dryAlertsDF.select(to_json(struct("*")).as("value"))
    .selectExpr("CAST(value AS STRING)")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServer)
    .option("topic", "dry_alerts")
    .save()
  sparkSession.close
}