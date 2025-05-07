package part7bigdata

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object TaxiApplication extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()
  import spark.implicits._

  //val bigTaxiDF = spark.read.load("path/to/your/dataset/NYC_taxi_2009-2016.parquet")

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiDF.printSchema()

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")
  taxiZonesDF.printSchema()

  //taxiDF.show()
  //taxiZonesDF.show()

  /**
    * Questions:
    *
    * 1. Which zones have the most pickups/dropoffs overall?
    * 2. What are the peak hours for taxi?
    * 3. How are the trips distributed by length? Why are people taking the cab?
    * 4. What are the peak hours for long/short trips?
    * 5. What are the top 3 pickup/dropoff zones for long/short trips?
    * 6. How are people paying for the ride, on long/short trips?
    * 7. How is the payment type evolving with time?
    * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
    *
    */


  /**
    * taxiDF
    *  |-- VendorID: integer (nullable = true)
    *  |-- tpep_pickup_datetime: timestamp (nullable = true)
    *  |-- tpep_dropoff_datetime: timestamp (nullable = true)
    *  |-- passenger_count: integer (nullable = true)
    *  |-- trip_distance: double (nullable = true)
    *  |-- RatecodeID: integer (nullable = true)
    *  |-- store_and_fwd_flag: string (nullable = true)
    *  |-- PULocationID: integer (nullable = true)
    *  |-- DOLocationID: integer (nullable = true)
    *  |-- payment_type: integer (nullable = true)
    *  |-- fare_amount: double (nullable = true)
    *  |-- extra: double (nullable = true)
    *  |-- mta_tax: double (nullable = true)
    *  |-- tip_amount: double (nullable = true)
    *  |-- tolls_amount: double (nullable = true)
    *  |-- improvement_surcharge: double (nullable = true)
    *  |-- total_amount: double (nullable = true)
    *
    *
    * taxiZonesDF
    *  |-- LocationID: integer (nullable = true)
    *  |-- Borough: string (nullable = true)
    *  |-- Zone: string (nullable = true)
    *  |-- service_zone: string (nullable = true)
    */

  //1a
  val pickupsByTaxiZoneDF = taxiDF.groupBy("PULocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("totalTrips").desc_nulls_last)

  ///pickupsByTaxiZoneDF.show(false)

  //1b extra question to evaluate total trips on column Borough
  pickupsByTaxiZoneDF.groupBy("Borough")
    .agg(sum(col("totalTrips")).alias("totalTripsByBorough"))
    .orderBy(col("totalTripsByBorough").desc_nulls_last)

  //2

  val pickupsByHourDF = taxiDF.withColumn("pick_By_Hour", hour(col("tpep_pickup_datetime")))
    .groupBy(col("pick_By_Hour"))
    .agg(count(col("*")).alias("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  //3
  val distanceDF = taxiDF.select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30
  val tripDistanceStatsDF = distanceDF.select(
    count("*").alias("count"),
    lit("longDistanceThreshold").as("threshold"),
    mean(col("distance").as("mean")),
    stddev(col("distance")).as("stddev"),
    min(col("distance")).as("min"),
    max(col("distance")).as("max")
  )

  val tripsWithLengthDF = taxiDF.withColumn("isLong",col("trip_distance") > longDistanceThreshold)
  tripsWithLengthDF.groupBy("isLong").agg(count("*"))

  //4

  val pickupsByHourByLengthDF = tripsWithLengthDF.withColumn("pick_By_Hour", hour(col("tpep_pickup_datetime")))
    .groupBy(col("pick_By_Hour"),col("isLong"))
    .agg(count(col("*")).alias("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
  //pickupsByHourByLengthDF.show(100)

  //5

  def pickupDropoffPopularity(predicate: Column) = tripsWithLengthDF.
    where(predicate)
    .groupBy(col("PULocationID"),col("DOLocationID"))
    .agg(count("*").alias("total_trips"))
    .join(taxiZonesDF,col("PULocationID") === col("LocationID"))
    .withColumnRenamed("Zone","Pickup_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .join(taxiZonesDF,col("DOLocationID") === col("LocationID"))
    .withColumnRenamed("Zone","Dropoff_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .drop("PULocationID","DOLocationID")
    .orderBy(col("total_trips").desc_nulls_last)

  pickupDropoffPopularity(col("isLong")) //Long Trip Calculation
  pickupDropoffPopularity(not(col("isLong"))) //Short Trip Calculation

  //6

  tripsWithLengthDF.groupBy("isLong","RatecodeID")
    .agg(count("*").alias("TotalTrips"))
    .orderBy(col("isLong"),col("TotalTrips").desc)

  //7
  taxiDF.groupBy(
    to_date(col("tpep_pickup_datetime")).alias("pick_up_date"),
    col("RatecodeID"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("pick_up_date"))

  //8

  val groupAttemptsDF = taxiDF.select(
    round(unix_timestamp(col("tpep_pickup_datetime"))/300).cast("integer"
    ).as("fiveMinId"),col("PULocationID"),col("total_amount"))
    .where(col("passenger_count") < 3)
    .groupBy("fiveMinId","PULocationID")
    .agg(count("*").alias("totalTrips"),sum(col("total_amount")).alias("total_amount"))
    .orderBy(col("totalTrips").desc_nulls_last)
    .withColumn("approximate_date_time",from_unixtime(col("fiveMinId")*300))
    .drop("fiveMinId")
    .join(taxiZonesDF,col("LocationID") === col("PULocationID"))
    .drop("service_zone","LocationID")

  // Additional use case question for Economic Impact analysis
  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extraCost = 2
  val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("totalTrips") * percentGroupAttempt)
    .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

  val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
  // 40k/day = 12 million/year!!!


}
