package etl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class HotWordEtl {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("hotwordetl")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.setLogLevel("WARN");
//        JavaRDD<String> lines = sc.textFile("D:\\work\\IdeaProjects\\EcommerceUserProfile\\src\\text.txt");

//        System.out.println(lines.count());
//        JavaRDD<String> linesRdd = jsc.textFile("D:\\work\\IdeaProjects\\EcommerceUserProfile\\src\\text.txt");
        JavaRDD<String> linesRdd = sc.textFile("E:\\test\\SogouQ1.txt");
        JavaPairRDD<String, Integer> pairRDD = linesRdd.mapToPair(str->{
            String[] split = str.split("\t");
            if (split.length > 2) {
                return new Tuple2<>(split[2],1);
            }else{
                System.out.println(str);
                return new Tuple2<>("null",1);
            }
        });
//                20111230035936	2ef3857a20b61881631b0c47802838b5	水浒无双五行伤害	2	2	http:78dsafbbs.92wy.com/thread-19095399-2-1.html

        JavaPairRDD<String, Integer> countRdd = pairRDD.reduceByKey((w1, w2) -> w1 + w2);

        JavaPairRDD<Integer, String> swapedRdd = countRdd.mapToPair(tup -> tup.swap());

        //倒序排
        JavaPairRDD<Integer, String> sortedRdd = swapedRdd.sortByKey(false);

        JavaPairRDD<String, Integer> resultRdd = sortedRdd.mapToPair(tup -> tup.swap());

        List<Tuple2<String, Integer>> hotWordCounts = resultRdd.take(10);

        for (Tuple2<String, Integer> hotWordCount : hotWordCounts) {
            System.out.println(hotWordCount);
        }
    }
}
