# sparkts-forecast

Forecast stocks with ARIMA model using [spark-timeseries 0.3.0](https://github.com/sryza/spark-timeseries)
<br>
Read .csv files using [spark-csv](spark-csv)

#### Prerequisite
* Java JDK 6+
* Scala 2.11.8
* Maven 3.3.9
* Hadoop 2.6.2
* Spark 1.6.0

#### Use Maven for building
    mvn clean package

#### Submit to spark
    spark-submit --packages com.databricks:spark-csv_2.10:1.4.0 --class TimeSeriesForecast /YOUR/PATH/TO/sparkts-forecast/arima/target/arima-1.0-SNAPSHOT-jar-with-dependencies.jar
