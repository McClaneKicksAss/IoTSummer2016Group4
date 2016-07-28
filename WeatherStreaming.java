package iot.project;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.cts.crs.CRSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import de.uniluebeck.itm.util.logging.Logging;
import iot.project.CoordinateTransformExample.CoordinateTransformHelper;
import scala.Tuple2;
import scala.Tuple4;

public class WeatherStreaming {
	private static final String host = "localhost";
	
	//generate constant values
	private static final String LATITUDE = "latitude";
	private static final String LONGITUDE = "longitude";
	private static final String WEATHER_TYPE = "type";
	private static final String WEATHER_TYPE_VALUE = "value";
	
	private static final String RAIN = "rain";
	private static final String TEMPERATURE = "temperature";
	private static final String WIND = "wind";
	
	//min and max values for data generation
	private static final int MIN_RAIN = 0;
	private static final int MAX_RAIN = 1500;
	private static final int MIN_TEMPERATURE = -20;
	private static final int MAX_TEMPERATURE = 40;
	private static final int MIN_WIND = 0;
	private static final int MAX_WIND = 12;
	
	//min and max values for latitude and longitude representing the geographical borders of Canada
	private static final double MIN_LAT = 41.681389;
	private static final double MAX_LAT = 83.111389;
	private static final double MIN_LON = -141.001944;
	private static final double MAX_LON = -52.619444;
	
	private static final double GRID_ELEMENTS = 450;

	//Map for storing analysis results in - values will be sent to webserver for displaying the smiley face
	static Map<String, Double> smileyValueMap = new HashMap<String, Double>();
		
	//interface providing get method for generating weather data
	interface WeatherProvider {
		String get(double lat, double longitude);
	}

	//class for generating rain weather data
	static class Rain implements WeatherProvider {
		
		Random r = new Random();

		@Override
		public String get(double lat, double longitude) {
			int rainValue = r.nextInt(MAX_RAIN + 1);
			String data = Double.toString(lat) + "," + Double.toString(longitude) + "," + RAIN + "," + Integer.toString(rainValue);
			//System.out.println(data);
			return data;
		}
	}

	//class for generating temperature weather data
	static class Temperature implements WeatherProvider {
		
		Random r = new Random();

		@Override
		public String get(double lat, double longitude) {
			int temperatureValue = r.nextInt((MAX_TEMPERATURE - MIN_TEMPERATURE) + 1) + MIN_TEMPERATURE;
			String data = Double.toString(lat) + "," + Double.toString(longitude) + "," + TEMPERATURE + "," + Integer.toString(temperatureValue);
			//System.out.println(data);
			return data;
		}
	}
	
	//class for generating wind weather data
	static class Wind implements WeatherProvider {
		
		Random r = new Random();

		@Override
		public String get(double lat, double longitude) {
			int windValue = r.nextInt(MAX_WIND);
			String data = Double.toString(lat) + "," + Double.toString(longitude) + "," +  WIND + "," + Integer.toString(windValue);
			//System.out.println(data);
			return data;
		}
	}
	
	/**
	 * method for generating a key out of the coordinates
	 * @param latitude: latitude value of the coordinates for which the x-y key should be created
	 * @param longitude: longitude value of the coordinates for which the x-y key should be created
	 * @return String key "x-y": consisting of x and y values which were generated out of latitude and longitude
	 * @throws Exception
	 */
	public static String generateXYkey(double latitude, double longitude) throws Exception{
		//set up GridHelper
		//System.out.println("Generate key for: lat " + latitude + " long " + longitude);
		CoordinateTransformHelper transformation = new CoordinateTransformHelper("EPSG:4326", "EPSG:3857"); 
		double[] minLonLat = {MIN_LON, MIN_LAT};
		double[] maxLonLat = {MAX_LON, MAX_LAT};
		GridHelper gridHelper = new GridHelper(transformation, minLonLat, maxLonLat, GRID_ELEMENTS);
		
		int[] xy = gridHelper.toGrid(latitude, longitude);
		String key = xy[0] + " - " + xy[1];
		//System.out.println(key);
		return key;
	}
	
	/**
	 * method for calculating the smiley value for one data line
	 * @param weatherType: either "rain", "wind" or "temperature"
	 * @param weatherTypeValue: the value of the weatherType
	 * @return smiley value of the weatherTypeValue - either -1, 0 or 1
	 */
	private static double calculateWeatherValue(String weatherType, String weatherTypeValue) {
		double value = Double.parseDouble(weatherTypeValue);
		double returnValue = 0;
		//set -1, 0 or 1 as returnValue depending on the weatherType and its value
		switch (weatherType)
		{
		case RAIN:
			if((value>=200 && value<400) || (value>800 && value<=1300)){
				returnValue = 0;
			}else if(value>=400 && value<=800){
				returnValue = 1;
			}else if(value<200 || value>1300){
				returnValue = -1;
			}
			break;
		case TEMPERATURE:
			if(value>=-5 && value<=15){
				returnValue = 0;
			}else if(value>15 && value<=35){
				returnValue = 1;
			}else if(value<-5 || value>35){
				returnValue = -1;
			}
			break;
		case WIND:
			if(value>5 && value<=9){
				returnValue = 0;
			}else if(value<=5){
				returnValue = 1;
			}else if(value > 9){
				returnValue = -1;
			}
			break;
		}
		//System.out.println(weatherType + " " + returnValue);
		return returnValue;
	}
	
	/**
	 * main function for streaming and analyzing the data with spark 
	 * @throws Exception
	 */
	public static void streamData() throws Exception {
		//set up the logging
		Logging.setLoggingDefaults();
		Logger log = LoggerFactory.getLogger(WeatherStreaming.class);
	
		//set up for random data generation
		WeatherProvider providers[] = new WeatherProvider[] {new Rain(), new Temperature(), new Wind()};
		Random r = new Random();

		// Create a server socket that produces random weather data; sleep for 50 milli seconds after one data set
		// was created
		ServerSocketSource<String> dataSource = new ServerSocketSource<>(() -> {
			double latitude = Math.random()*(MAX_LAT - MIN_LAT) + MIN_LAT;
			double longitude = (Math.random()*(MIN_LON*(-1) + MAX_LON) - MAX_LON)*(-1);
			return providers[r.nextInt(providers.length)].get(latitude, longitude);
			
		},() -> 50);

		// Create the spark context with a 3 second batch size
		SparkConf sparkConf = new SparkConf().setAppName("WeatherStreaming").setMaster("local[2]");
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

		// Create a JavaReceiverInputDStream on target ip:port and count the words in input stream of \n delimited text
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream(host, dataSource.getLocalPort(), StorageLevels.MEMORY_AND_DISK_SER);

				
		// Map String lines of the raw data to a key value pair
		JavaPairDStream<String, Double> mappedLines = lines.mapToPair(line -> {
			String[] values = line.split(",");
			String key = generateXYkey(Double.parseDouble(values[0]), Double.parseDouble(values[1]));
			double weatherValue = calculateWeatherValue(values[2], values[3]);
			//System.out.println(values[2] + " " + weatherValue);
			return new Tuple2<>(key, weatherValue);
		});
		
		// sum up the weather values for each key/cell on the grid
		JavaPairDStream<String, Double> smileyValues = mappedLines.reduceByKey((a, b) -> a + b);
		
		// print the key value pairs consisting of the cell key ("x-y") and the smileyValue (-1, 0 or 1)
		smileyValues.print();
		
		// save RDDs in smileyValueMap for the communication to the webserver
		smileyValues.foreachRDD(n ->{
			n.foreach(x -> {
				smileyValueMap.put(x._1, x._2);
			});
		});
		
		// Start the computation
		ssc.start();

		ssc.awaitTermination();
		ssc.close();
		dataSource.stop();
	}
}
