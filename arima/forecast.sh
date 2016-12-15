hadoop fs -rm -r /output/*

spark-submit --class TimeSeriesForecastScala ./target/arima-1.0-SNAPSHOT-jar-with-dependencies.jar 8
