package project.services
import org.apache.spark.sql.functions.{avg, col, from_json, struct, sum, to_json}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Encoders, SparkSession}
import project.model.{Farm, NormalizedStationCollectedData}
import project.services.CalcAlerts.{DF, bootstrapServer, farms, warmAlertsDF}
import project.services.KafkaManage.getOffsets


//91043
object temp extends App {
  val b=getOffsets("test")
  val bootstrapServer="cnt7-naya-cdh63:9092"
  val offsets= KafkaManage.getOffsets("weather_info_verified_data")

  val old_warms=KafkaManage.getOffsets("warm_alerts")
  val dry_warms=KafkaManage.getOffsets("dry_alerts")

  val sparkSession: SparkSession = SparkSession.builder.master("local[*]").appName("example_app").getOrCreate
  val farmSchema = Encoders.product[Farm].schema
  val farms = sparkSession.read.schema(farmSchema).json("farms\\*")

  val schema = Encoders.product[NormalizedStationCollectedData].schema
  val inputDF=sparkSession
    .read.format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServer)
    .option("subscribe", "weather_info_verified_data")
    .option("startingOffsets", """{"weather_info_verified_data":{"0":375500}}""")
    .load.select(col("value").cast(StringType).alias("value"))
  val DF = inputDF
    .withColumn("parsed_json", from_json(col("value"), schema))
    .select("parsed_json.*")


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
    .option("topic", "warm_alerts")
    .save()

  dryAlertsDF.select(to_json(struct("*")).as("value"))
    .selectExpr("CAST(value AS STRING)")
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServer)
    .option("topic", "dry_alerts")
    .save()

  val dd=KafkaManage.getOffsets("test")
  println("ff")
}



