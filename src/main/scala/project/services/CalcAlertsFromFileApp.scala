package project.services

import project.model.{Farm, NormalizedStationCollectedData}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{avg, col, current_date, current_timestamp, date_sub, element_at, from_json, lit, struct, sum, to_json}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.springframework.kafka.config.TopicBuilder


object CalcAlertsFromFileApp extends App {
  val bootstrapServer = "https://35.225.147.228:9092";
  val sparkSession: SparkSession = SparkSession.builder.master("local[*]").appName("example_app").getOrCreate
  val farmSchema = Encoders.product[Farm].schema
  val farms = sparkSession.read.schema(farmSchema).json("farms\\*")



  val schema = Encoders.product[NormalizedStationCollectedData].schema
  val measurementDF = sparkSession.read.schema(schema).json("data\\*")

  val pre= DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now.minusHours(90)).replace("T"," ")
  val measurementTimeDF = measurementDF.withColumn("s",measurementDF.col("datetime").cast("timestamp"))
  val DF=measurementTimeDF.filter(measurementTimeDF.col("s").gt(lit(pre)))

  //aggregate weather measurement and extend it to growing fields
  val aggDF=DF.groupBy(col("stationId"),col("channel")).agg(avg(col("value")),sum("value"))
  val heatDF=DF.filter(col("channel")==="TG").select(col("stationId"),col("value").as("warm_level"))
  val windDF= DF.filter(col("channel")==="WS").select(col("stationId"),col("value").as("wind_level"))
  val fieldsExtended=heatDF.join(windDF,"stationId").join(farms,"stationId")
  val DfWithDry= fieldsExtended.withColumn("dry_level",col("warm_level")+col("wind_level"))


  val DfWithHandle=DfWithDry.withColumn("dry_handle",col("dry_sensitivity")-col("dry_level")).
    withColumn("warm_handle",col("warm_sensitivity")-col("warm_level"))
  val warmAlertsDF=DfWithHandle.filter(col("warm_handle")<0).select(col("email"),col("name"),col("crops"))

  warmAlertsDF.select(to_json(struct("*")).as("value"))
    .selectExpr("CAST(value AS STRING)")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServer)
    .option("topic", "dry_alerts")
    .save()

    sparkSession.close
}

