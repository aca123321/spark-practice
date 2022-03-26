package com.aca123321;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        List<Double> inputData = Arrays.asList(1.0, 2.0, 3.0, 3.0, 4.5);

        SparkConf conf = new SparkConf().setAppName("Basic rdd setup").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Double> inputRDD = sc.parallelize(inputData);

        JavaRDD<Double> doubledRDD = inputRDD.map((Double a) -> {return a*2.00;});
        Double sum = doubledRDD.reduce((Double a, Double b) -> {return a+b;});

        System.out.println("Sum of twice of all the input numbers = " + sum);

        sc.close();
    }

}
