package com.aca123321;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class Main {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Basic rdd setup").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputRDD = sc.textFile("src/main/resources/subtitles/input.txt");

        JavaRDD<String> words = inputRDD.flatMap((String s) -> {
            ArrayList<String> ret = new ArrayList<>();
            for(String t: s.replaceAll("[^a-zA-Z\\s]", "").toLowerCase().trim().split(" |,|\\.|-|/|\"|=")) {
                if(t.length() != 0) {
                    ret.add(t);
                }
            }
            return ret.iterator();
        }).filter((String s) -> {
            return (!Character.isDigit(s.charAt(0))) && (!Util.isBoring(s));
        });

        JavaPairRDD<String, Long> wordPairRDD = words.mapToPair((String s) -> {return new Tuple2<String, Long>(s, 1L);});
        JavaPairRDD<String, Long> wordFreq = wordPairRDD.reduceByKey((Long a, Long b) -> {return a+b;});
        JavaPairRDD<Long, String> freqWord = wordFreq.mapToPair((Tuple2<String, Long> a) -> {
            return new Tuple2<>(a._2, a._1);
        });

        JavaPairRDD<Long, String> result = freqWord.sortByKey(false);

        for(Tuple2<Long, String> a: result.take(10)) {
            System.out.println(a._1 + ": " + a._2);
        }

        sc.close();
    }

}

class SortByFreq implements Comparator<Tuple2<String, Long>> {

    // Sorting in ascending order of frequency
    public int compare(Tuple2<String, Long> a, Tuple2<String, Long> b)
    {
        return (int)(a._2 - b._2);
    }
}