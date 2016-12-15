import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Date
import java.text.SimpleDateFormat
import java.lang.Math

import com.cloudera.sparkts._
import com.cloudera.sparkts.models.ARIMA

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.mllib.linalg.{Vector, Vectors}


object TimeSeriesForecastScala{
	val alpha = 1 - Math.exp(-0.30103 / 4.0)


	/*==========================================
	Exponential weighted moving average
	==========================================*/
	def ewma(v: Vector): Vector = {
		val data: Array[Double] = new Array[Double](v.size)
		data(0) = v.apply(0)

		for(i <- 1 until v.size)
			data(i) = (1 - alpha) * data(i-1) + alpha * v.apply(i)

		return Vectors.dense(data)
	}


	/*========================================
	Difference between v1 & v2
	========================================*/
	def diff(v1: Vector, v2: Vector): Vector = {
		val data: Array[Double] = new Array[Double](v1.size)

		for(i <- 0 until v1.size)
			data(i) = v1.apply(i) - v2.apply(i)

		return Vectors.dense(data)
	}


	/*========================================
	Read csv as a DataFrame
	========================================*/
   def readCSV(symbol: String, curDate: String, prevDate: String, secs: Long, customSchema: StructType, sc_sql: SQLContext): DataFrame = {
		//Read data
		var df_input = sc_sql.read
						 	 .format("com.databricks.spark.csv")
						 	 .option("header", "true")
						 	 .schema(customSchema)
						 	 .load("/input/" + prevDate + "_" + symbol + ".csv")

		val df_input_cur = sc_sql.read
						 	 	 .format("com.databricks.spark.csv")
						 	 	 .option("header", "true")
						 	 	 .schema(customSchema)
						 	 	 .load("/input/" + curDate + "_" + symbol + ".csv")


		//Union current & prevous data
		val inputRDD = df_input.map{r =>
			val t = r.getAs[Timestamp](0)
			t.setTime(t.getTime() + secs)
			Row(t, r.getAs(1), r.getAs(2))
		}
		df_input = sc_sql.createDataFrame(inputRDD, customSchema)
		
		return df_input.unionAll(df_input_cur)
   }


	/*========================================
	Forecast stock
	========================================*/
	def forecastStock(df: DataFrame, count_fc: Long, sc: SparkContext, sc_sql: SQLContext): DataFrame = {
		if(df.count() == 0) return sc_sql.createDataFrame(sc.emptyRDD[Row], new StructType())

		val latestTime = df.collect().last.getAs[Timestamp](0)
		val latestDate = ZonedDateTime.of(LocalDateTime.parse(latestTime.toString(),
															DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.0")), 
															ZoneId.systemDefault())
		val startDate = latestDate.minusMinutes(df.count())
		val startDate_fc = latestDate.plusMinutes(1)
		val latestDate_fc = latestDate.plusMinutes(count_fc)


		//Create DateTimeIndices
		val dtIndex_input = DateTimeIndex.uniformFromInterval(startDate, latestDate, new MinuteFrequency(1))
		val dtIndex_fc = DateTimeIndex.uniformFromInterval(startDate_fc, latestDate_fc, new MinuteFrequency(1))


		//Align data on the DateTimeIndex to create a TimeSeriesRDD
		val tsRDD = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex_input, df, "time", "stock", "price")
		tsRDD.cache()

		
		//Fill time series data
		val filledTsRDD = tsRDD.fill("linear").fill("next")


		//ARIMA forecast
		val tsRDD_forecast = filledTsRDD.mapSeries({ts =>
			val data: Array[Double] = Array.fill[Double](count_fc.toInt)(0)	

			try{
				val ts_ewma = ewma(ts)
				val ts_ewma_diff = diff(ts, ts_ewma)
				val ts_diff_fc = ARIMA.fitModel(1, 0, 2, ts_ewma_diff).forecast(ts_ewma_diff, count_fc.toInt)

				data(0) = ts_ewma.apply(ts_ewma.size - 1) + ts_diff_fc.apply(ts_diff_fc.size - count_fc.toInt) / (1 - alpha)

				for(i <- 1 until count_fc.toInt)
					data(i) = data(i-1) + ts_diff_fc.apply(ts_diff_fc.size - count_fc.toInt + i) / (1 - alpha)
			}catch{
				case e: Exception =>
			}

			Vectors.dense(data)
		}, dtIndex_fc)

		return tsRDD_forecast.toObservationsDataFrame(sc_sql, "time", "stock", "price")
	}


	def main(args: Array[String]): Unit = {
		
		val count_forecast = 8


		//Create a spark context
		val conf = new SparkConf().setAppName("ARIMA Forecast")
		conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
		val sc = new SparkContext(conf)
		val sc_sql = new SQLContext(sc);


		//Create a dataframe schema
		val fields = Seq(
				StructField("time", TimestampType, true),
				StructField("stock", StringType, true),
				StructField("price", DoubleType, true)
		)
		val customSchema = StructType(fields)


		//Read data
		val stockSymbol = sc.textFile("/input/stockSymbols").collect()
		val stockDate = sc.textFile("/input/stockDate").collect()

		if(stockSymbol.isEmpty){
			println("ERROR: No input symbol")
			exit(1)
		}

		if(stockDate.size < 2){
			println("ERROR: No input date")
			exit(1)
		}


		//Get current date
		val date = new Date()
		val curDate = stockDate(stockDate.size - 1)
		val prevDate = stockDate(stockDate.size - 2)

		println("Current date: " + curDate)

		val dateFormat = new SimpleDateFormat()
		dateFormat.applyPattern("yyyyMMdd")

		val date_cur = dateFormat.parse(curDate)
		val date_prev = dateFormat.parse(prevDate)
		val secs = date_cur.getTime() - date_prev.getTime() - 16200000
		
		//Read data
		var df_input = readCSV(stockSymbol(0), curDate, prevDate, secs, customSchema, sc_sql)

		for(i <- 1 until stockSymbol.length){
			val df_input_tmp = readCSV(stockSymbol(i), curDate, prevDate, secs, customSchema, sc_sql)
			df_input = df_input.unionAll(df_input_tmp)
		}
	
		//Log transform
		val inputRDD = df_input.map{r =>
			Row(r.getAs(0), r.getAs(1), Math.log10(r.getAs(2)))
		}
		df_input = sc_sql.createDataFrame(inputRDD, customSchema)

		//Forecast stock
		val df_fc = forecastStock(df_input, count_forecast, sc, sc_sql)

		//Exp transform
		val outputRDD = df_fc.map{r =>
			Row(r.getAs(0), r.getAs(1), Math.pow(10, r.getAs(2)))
		}
		val df_output = sc_sql.createDataFrame(outputRDD, customSchema)

		//Print data
		df_output.foreach{row =>
			println(row.getAs(0).toString + "," + row.getAs(1).toString + "," + row.getAs(2).toString)
		}

		//Save data
		//for(i <- 0 until stockSymbol.length){
		//	val df = df_output.filter("stock = '" + stockSymbol(i) + "'")
		//	df.show()
		//}
	}
}
