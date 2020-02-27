package com.shalini.apache.spark;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkWordCount {
	 private static final Pattern SPACE = Pattern.compile(" ");
public static void main(String[] args) throws Exception{
	SparkSession spark=SparkSession.builder().appName("Spark Word count program").master("local[2]").getOrCreate();
	JavaRDD<String> rd=spark.read().textFile("D:\\project\\git\\spark\\spark\\src\\main\\resource\\input\\test.txt").javaRDD();
	
	JavaRDD<String> words = rd.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> word = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> wordcount = word.reduceByKey((i1, i2) -> i1 + i2);

    List<Tuple2<String, Integer>> output = wordcount.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }
    spark.stop();
}
}
