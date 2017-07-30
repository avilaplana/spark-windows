package dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}

case class Temperature(city: String, day: Integer, temperature: Double)

object TemperatureCycle {

  val ss = SparkSession
    .builder()
    .appName("Temperature measurement")
    .config("spark.master", "local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    import ss.implicits._

    val temperatures: DataFrame = ss.sparkContext.parallelize(Seq(
      Temperature("London", 1, 5),
      Temperature("London", 2, 7),
      Temperature("London", 3, 9),
      Temperature("London", 4, 12),
      Temperature("London", 5, 13),
      Temperature("London", 6, 6),
      Temperature("London", 7, 10),
      Temperature("London", 8, 6),
      Temperature("London", 9, 11),
      Temperature("London", 10, 13),
      Temperature("London", 11, 10)
    )).toDF()

    temperatures.show(false)


    import org.apache.spark.sql.functions._

    import org.apache.spark.sql.expressions.Window

    val w = Window
      .partitionBy(col("city"))
      .orderBy(col("day"))

    val tempWithPreviousTemp = temperatures
      .withColumn("inCycle", when(col("temperature") >= 10, true).otherwise(false))
      .withColumn("previousTemperature", lag("temperature", 1).over(w))
      .withColumn("initialCycle", when(col("inCycle") && col("previousTemperature").isNotNull && col("previousTemperature") < 10, col("day")).otherwise(null))
      .withColumn("startCycle", last("initialCycle", true).over(w.rangeBetween(Long.MinValue, 0)))
      .where(col("inCycle"))
      .drop("inCycle", "previousTemperature", "initialCycle")

    tempWithPreviousTemp.show(false)
  }

}
