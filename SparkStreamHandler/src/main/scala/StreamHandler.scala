import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._


//case class DeviceData(time_stamp: Double, device: String, x: Integer, y: Integer, z: Integer)
case class Point(x: Int, y:Int)
//case class DeviceData(time_stamp: Double, device: String, start_x: Integer, start_y: Integer, end_x: Integer, end_y: Integer)
case class DeviceData(time_stamp: Double, device: String, start_point: Point, end_point: Point)

object StreamHandler {
	def main(args: Array[String]) {
		val spark = SparkSession
			.builder
			.appName("StreamHandler")
			.getOrCreate()

		import spark.implicits._

		val inputDF = spark
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", "localhost:9092")
			.option("subscribe", "location")
			.load()

		val rawDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]

		val formattedDF = rawDF.map(row => row.split(","))
			.map(
				row => DeviceData(
					row(0).trim.toDouble,
					row(1),
					Point(row(2).trim.toInt, row(3).trim.toInt),
					Point(row(4).trim.toInt, row(5).trim.toInt)	
				)
			)

		val distanceDF = formattedDF.withColumn("distance", calculate_distance(col("start_point"), col("end_point")))

		val aggDF = distanceDF
			.groupBy("device")
			.agg(
				sum("distance").as("Total distance"),
				avg("distance").as("Average trip distance"),
				min("distance").as("Shortest Trip"),
				max("distance").as("Longest Trip"),
				count(lit(1).as("Number of trips"))
			)

		val query = aggDF
			.writeStream
			.trigger(Trigger.ProcessingTime("10 seconds"))
			.outputMode("update")
			.format("console")
			.start()

		query.awaitTermination()
	}

	def calculate_distance_func(start: Point, end: Point) : Double = {
		var distance:Double = 0
		distance = ((start.x - end.x).abs + (start.y - end.y).abs).abs
		return distance
	}

	val calculate_distance = udf[Double, Point, Point](calculate_distance_func)
}
