package com.zx.Spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WcApp {
	
	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf();
		conf.setMaster("local[4]");
		conf.setAppName("wordcount");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaRDD<String> rdd = context.textFile("d:/a.txt");
		JavaRDD<String> words = rdd.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String arg0) throws Exception {
				return Arrays.asList(arg0.split(" ")).iterator();
			}
			      
		});
		
		JavaPairRDD<String, Integer> count = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}}).reduceByKey(new Function2<Integer, Integer, Integer>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Integer call(Integer v1, Integer v2) throws Exception {
					return v1 + v2;
				}
			});
		
		count.saveAsTextFile("d:/b.txt");	
		context.close();
	}
		
}
