import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.util.Utils;


import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WordFreqGt {
    public static void main(String[] args) {
        // Configure Spark
        SparkConf conf = new SparkConf()
                .setAppName("Word Frequency")
                .set("spark.default.parallelism", "5000"); // Increase number of partitions

        // Create Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        

        // Load the dataset
        JavaRDD<String> lines = sc.textFile("/Users/pralhad/Big_Data/dataset_updated/data_16GB.txt");
        
        // Get the start time
        long startTime = System.currentTimeMillis();

        // Convert the text to lowercase and split into words
        JavaRDD<String> words = lines.flatMap(line ->
                Arrays.asList(line.toLowerCase().split("\\W+")).iterator());

        // Load the stop words from file
        Set<String> stopWords;
        try {
            stopWords = new HashSet<>(Files.readAllLines(Paths.get("/Users/pralhad/Big_Data/Stopwords.txt")));
        } catch (IOException e) {
            System.err.println("Failed to read stop words file: " + e.getMessage());
            sc.stop();
            return;
        }

        // Filter out stop words
        JavaRDD<String> filteredWords = words.filter(word -> word.length() > 6 && !stopWords.contains(word));

        // Count the frequency of each word
        JavaPairRDD<String, Long> wordCounts = filteredWords
                .mapToPair(word -> new Tuple2<>(word, 1L))
                .reduceByKey(Long::sum);

        // Get the top 100 most frequent words
        List<Tuple2<Long, String>> topWords = wordCounts
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .take(100);

        // Print the top 100 words with their frequencies
        for (Tuple2<Long, String> word : topWords) {
            System.out.println(word._1() + ": " + word._2());
        }    
    
        // Get the end time
        long endTime = System.currentTimeMillis();        
        System.out.println("Execution time: " + (endTime - startTime) + " ms");   
     


        // Stop Spark context
        sc.stop();
    }
}
