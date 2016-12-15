import com.cloudera.sparkts.models.ARIMA;
import com.cloudera.sparkts.models.ARIMAModel;
import com.cloudera.sparkts.models.EWMA;
import com.cloudera.sparkts.models.EWMAModel;
import com.cloudera.sparkts.MinuteFrequency;
import com.cloudera.sparkts.DateTimeIndex;
import com.cloudera.sparkts.api.java.DateTimeIndexFactory;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDD;
import com.cloudera.sparkts.api.java.JavaTimeSeriesRDDFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Calendar;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.lang.Math;


public class TimeSeriesForecast{
	public static final double alpha = 1 - Math.exp(-0.30103 / 4.0);

	/*==========================================
	Get last n elements of a vector
	===========================================*/
	public static Vector getLast(Vector v, int n)
	{
		double[] data = new double[n];

		for(int i = 0; i < n; ++i)
			data[i] = v.apply(v.size() - n + i - 1);

		return Vectors.dense(data);
	}


	/*==========================================
	Exponentially weighted moving average
	===========================================*/
	public static Vector ewma(Vector v)
	{
		double[] data = new double[v.size()];

		data[0] = v.apply(0);

		for(int i = 1; i < v.size(); ++i)
			data[i] = (1 - alpha) * data[i-1] + alpha * v.apply(i);

		return Vectors.dense(data);
	}


	/*==========================================
	Differece between v1 & v2
	===========================================*/	
	public static Vector diff(Vector v1, Vector v2)
	{
		double[] data = new double[v1.size()];

		for(int i = 0; i < v1.size(); ++i)
			data[i] = v1.apply(i) - v2.apply(i);

		return Vectors.dense(data);
	}


	/*==========================================
	Get a row with previous time
	===========================================*/
	public static Row getRowWithPrevTime(Row r, long diff)
	{
		Timestamp t = r.getAs(0);
		t.setTime(t.getTime() + diff);
		return RowFactory.create(t, r.getAs(1), r.getAs(2));
	}


	/*==========================================
	Forecast stock data
	===========================================*/
	public static DataFrame forecastStock(DataFrame df, int interval, int count_forecast, int count_prev, ZoneId zone, SQLContext sc_sql)
	{
		if(df.count() == 0) return null;

		Row[] df_data = df.collect();
		Timestamp latestTime = df_data[df_data.length - 1].getAs(0);

		ZonedDateTime latestDate = ZonedDateTime.of(LocalDateTime.parse(latestTime.toString(), 
													DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.0")), zone);
		ZonedDateTime startDate = latestDate.minusMinutes(count_prev * interval);
		ZonedDateTime startDate_forecast = latestDate.plusMinutes(interval);
		ZonedDateTime latestDate_forecast = latestDate.plusMinutes(count_forecast * interval);


		//Create DateTimeIndices
		DateTimeIndex dtIndex_input = DateTimeIndexFactory.uniformFromInterval(
			startDate, latestDate, new MinuteFrequency(interval));

		DateTimeIndex dtIndex_forecast = DateTimeIndexFactory.uniformFromInterval(
			startDate_forecast, latestDate_forecast, new MinuteFrequency(interval));


		//Align data on the DateTimeIndex to create a TimeSeriesRDD
		JavaTimeSeriesRDD tsRDD = JavaTimeSeriesRDDFactory.timeSeriesRDDFromObservations(
			dtIndex_input, df, "time", "stock", "price");

		tsRDD.cache();


		//Filter time series data
		JavaTimeSeriesRDD<String> filledTsRDD = tsRDD.fill("linear").fill("next");


		//ARIMA forecast
		JavaTimeSeriesRDD<String> tsRDD_forecast;

		try{
			tsRDD_forecast = filledTsRDD.mapSeries(
				(Vector ts) -> {
					//Vector ts_ewma = Vectors.dense(new double[ts.size()]);	
					//EWMA.fitModel(ts).removeTimeDependentEffects(ts, ts_ewma);

					Vector ts_ewma = ewma(ts);
					Vector ts_ewma_diff = diff(ts, ts_ewma);
					Vector ts_diff_fc = getLast(ARIMA.fitModel(1, 0, 2,
											ts_ewma_diff, true, "css-cgd", null)
											.forecast(ts_ewma_diff, count_forecast), 
											count_forecast);

					double[] data = new double[count_forecast];
					data[0] = ts_ewma.apply(ts_ewma.size() - 1)
							+ ts_diff_fc.apply(0) / (1 - alpha);

					for(int i = 1; i < count_forecast; ++i)
						data[i] = data[i-1] + ts_diff_fc.apply(i) / (1 - alpha);
					
					return Vectors.dense(data);
				}
				, dtIndex_forecast);
		}catch(Exception e){
			return null;
		}
		
		return tsRDD_forecast.toObservationsDataFrame(sc_sql, "time", "stock", "price");
	}

	
	/*======================================
	Save a data frame as a csv file
	=======================================*/
	public static boolean saveDataFrameAsCSV(DataFrame df, String filePath)
	{
		try{
			df.write()
			  .format("com.databricks.spark.csv")
			  .option("header", "false")
			  .option("dateFormat", "yyyy-MM-dd HH:mm:ss")
			  .save(filePath);
		}catch(Exception e){
			return false;
		}
	
		return true;
	}


	public static void main(String args[])
	{
		if(args.length < 1){
			System.out.println("ERROR: Too few arguments");
			System.exit(1);
		}

		int count_forecast = Integer.parseInt(args[0]);
		if(count_forecast < 1){
			System.out.println("ERROR: count_forecast must be greater than 0");
			System.exit(1);
		}


		//Create a spark context
		SparkConf conf = new SparkConf().setAppName("ARIMA Forecast");
		conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sc_sql = new SQLContext(sc);


		//Create a data frame schema
		List<StructField> fields = new ArrayList();
		fields.add(DataTypes.createStructField("time", DataTypes.TimestampType, true));
		fields.add(DataTypes.createStructField("stock", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("price", DataTypes.DoubleType, true));
		StructType customSchema = DataTypes.createStructType(fields);
	
		
		//Read data
		List<String> stockSymbol = sc.textFile("/input/stockSymbols").collect();
		List<String> stockDate = sc.textFile("/input/stockDate").collect();

		if(stockSymbol.isEmpty()){
			System.out.println("ERROR: No input symbol");
			System.exit(1);
		}

		if(stockDate.size() < 2){
			System.out.println("ERROR: No input date");
			System.exit(1);
		}


		//Get current date
		Date date = new Date();
		String curDate = stockDate.get(stockDate.size() - 1);
		String prevDate = stockDate.get(stockDate.size() - 2);
		ZoneId zone = ZoneId.systemDefault();
		DataFrame df_input, df_input_cur, df_forecast;

		System.out.println("Current date: " + curDate);

		SimpleDateFormat dateFormat = new SimpleDateFormat();
		dateFormat.applyPattern("yyyyMMdd");
		Date date_cur = null, date_prev = null;

		try{
			date_cur = dateFormat.parse(curDate);
			date_prev = dateFormat.parse(prevDate);
		}
		catch(Exception e){
			System.out.println("ERROR: Failed to parse date format");
			System.exit(1);
		}

		long secs = date_cur.getTime() - date_prev.getTime() - 16200000;


		//Forecast each data
		for(int i = 0; i < stockSymbol.size(); ++i){
			//Read data
			df_input = sc_sql.read()
						 	 .format("com.databricks.spark.csv")
						 	 .option("header", "true")
						 	 .schema(customSchema)
						 	 .load("/input/" + prevDate + "_" + stockSymbol.get(i) + ".csv");

			df_input_cur = sc_sql.read()
						 		 .format("com.databricks.spark.csv")
						 		 .option("header", "true")
							 	 .schema(customSchema)
						 		.load("/input/" + curDate + "_" + stockSymbol.get(i) + ".csv");

			//Union current & last data
			JavaRDD<Row> inputRDD = df_input.javaRDD().map(
				(Row r) -> getRowWithPrevTime(r, secs)
			);
			df_input = sc_sql.createDataFrame(inputRDD, customSchema);
			df_input = df_input.unionAll(df_input_cur);

			//Log transform
			inputRDD = df_input.javaRDD().map(
				(Row r) -> RowFactory.create(r.getAs(0), r.getAs(1), Math.log10(r.getAs(2)))
			);
			df_input = sc_sql.createDataFrame(inputRDD, customSchema);
			
			//Forecast stock
			df_forecast = forecastStock(df_input, 1, count_forecast, (int)df_input.count(), zone, sc_sql);

			if(df_forecast == null)
				System.out.println("Failed to compute " + stockSymbol.get(i));
			else{
				//Exponential transform
				JavaRDD<Row> outputRDD = df_forecast.javaRDD().map(
					(Row r) -> RowFactory.create(r.getAs(0), r.getAs(1), Math.pow(10, r.getAs(2)))
				);
				df_forecast = sc_sql.createDataFrame(outputRDD, customSchema);

				//Save data
				if(!saveDataFrameAsCSV(df_forecast,
					"/output/" + curDate + "_" + stockSymbol.get(i) + ".fc.csv"))
				System.out.println("Failed to save " + stockSymbol.get(i));
			}
		}
	}
}
