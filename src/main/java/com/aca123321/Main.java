package com.aca123321;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        List<Double> inputData = Arrays.asList(1.0, 2.0, 3.0, 3.0, 4.5);

        SparkConf conf = new SparkConf().setAppName("Basic rdd setup").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> inputRDD = sc.parallelize(inputData);
        JavaPairRDD<Double, Integer> inputPairs = inputRDD.mapToPair((Double a) -> { return new Tuple2<Double, Integer>(a, 1);});

        JavaPairRDD<Double, Integer> freqRDD = inputPairs.reduceByKey((Integer a, Integer b) -> {return  a + b;});

        freqRDD.foreach(val -> {
            System.out.println("frequency of " + val._1 + ": " + val._2);
        });

        sc.close();
    }

}
