/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bigDataProject;

import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author Poyka
 */
public class MainClass {
     public static void main(String[] args) {
     
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local[*]");
        SparkContext sparkContext = new SparkContext(conf);
       JavaRDD<String> data1=  sparkContext.textFile("E:\\t.txt", 1).toJavaRDD();
       
         data1.flatMap(new FlatMapFunction<String, String>() {      

            @Override
            public Iterator<String> call(String t) throws Exception {
                String x[] = t.split(",");
                String st = x[1];
                return Arrays.asList(st).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String t) throws Exception {
                return new Tuple2<>(t, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer t1, Integer t2) throws Exception {
                return t1 + t2;
            }
        }).saveAsTextFile("D:\\output"); //action
    }
}
