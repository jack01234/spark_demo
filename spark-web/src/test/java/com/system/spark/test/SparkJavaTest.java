package com.system.spark.test;

import com.system.spark.Application;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class SparkJavaTest {

//    @Autowired
//    private Ignite ignite;

    private static final String CONFIG = "example-default.xml";

    @Test
    public void test() throws IOException {
//        log.info("ignite {}",ignite);
        {
            SparkSession spark = SparkSession
                    .builder()
                    .appName("DFWriter")
                    .master("local")
                    .config("spark.executor.instances", "2")
                    .getOrCreate();

            Logger.getRootLogger().setLevel(Level.OFF);
            Logger.getLogger("org.apache.ignite").setLevel(Level.OFF);

            Dataset<Row> peopleDF = spark.read().json(
                    resolveIgnitePath("D:\\project\\spark_demo\\spark-web\\src\\main\\resources\\people.json").getCanonicalPath());

            System.out.println("JSON file contents:");

            peopleDF.show();

            System.out.println("Writing DataFrame to Ignite.");

            peopleDF.write()
                    .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                    .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                    .option(IgniteDataFrameSettings.OPTION_TABLE(), "people")
                    .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS(), "id")
                    .option(IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS(), "template=replicated")
                    .save();

            System.out.println("Done!");

            Ignition.stop(false);
        }
    }

    @Test
    public void readTest(){
        SparkSession spark = SparkSession
                .builder()
                .appName("DFReader")
                .master("local")
                .config("spark.executor.instances", "2")
                .getOrCreate();

        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org.apache.ignite").setLevel(Level.OFF);

        System.out.println("Reading data from Ignite table.");

        Dataset<Row> peopleDF = spark.read()
                .format(IgniteDataFrameSettings.FORMAT_IGNITE())
                .option(IgniteDataFrameSettings.OPTION_CONFIG_FILE(), CONFIG)
                .option(IgniteDataFrameSettings.OPTION_TABLE(), "people")
                .load();

        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people WHERE id > 0 AND id < 10");
        sqlDF.show();

        System.out.println("Done!");

        Ignition.stop(false);
    }

    @Test
    public void igniteContextTest(){
        // Spark Configuration.
        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaIgniteRDDExample")
                .setMaster("local")
                .set("spark.executor.instances", "2");

        // Spark context.
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Adjust the logger to exclude the logs of no interest.
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger("org.apache.ignite").setLevel(Level.INFO);

        // Creates Ignite context with specific configuration and runs Ignite in the embedded mode.
        JavaIgniteContext<Integer, Integer> igniteContext = new JavaIgniteContext<Integer, Integer>(
                sparkContext,CONFIG, false);

        // Create a Java Ignite RDD of Type (Int,Int) Integer Pair.
        JavaIgniteRDD<Integer, Integer> sharedRDD = igniteContext.<Integer, Integer>fromCache("sharedRDD");

        // Define data to be stored in the Ignite RDD (cache).
        List<Integer> data = new ArrayList<>(20);

        for (int i = 0; i<20; i++) {
            data.add(i);
        }

        // Preparing a Java RDD.
        JavaRDD<Integer> javaRDD = sparkContext.<Integer>parallelize(data);

        // Fill the Ignite RDD in with Int pairs. Here Pairs are represented as Scala Tuple2.
        sharedRDD.savePairs(javaRDD.<Integer, Integer>mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override public Tuple2<Integer, Integer> call(Integer val) throws Exception {
                return new Tuple2<Integer, Integer>(val, val);
            }
        }));

        System.out.println(">>> Iterating over Ignite Shared RDD...");

        // Iterate over the Ignite RDD.
        sharedRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override public void call(Tuple2<Integer, Integer> tuple) throws Exception {
                System.out.println("(" + tuple._1 + "," + tuple._2 + ")");
            }
        });

        System.out.println(">>> Transforming values stored in Ignite Shared RDD...");

        // Filter out even values as a transformed RDD.
        JavaPairRDD<Integer, Integer> transformedValues =
                sharedRDD.filter(new Function<Tuple2<Integer, Integer>, Boolean>() {
                    @Override public Boolean call(Tuple2<Integer, Integer> tuple) throws Exception {
                        return tuple._2() % 2 == 0;
                    }
                });

        // Print out the transformed values.
        transformedValues.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override public void call(Tuple2<Integer, Integer> tuple) throws Exception {
                System.out.println("(" + tuple._1 + "," + tuple._2 + ")");
            }
        });

        System.out.println(">>> Executing SQL query over Ignite Shared RDD...");

        // Execute SQL query over the Ignite RDD.
        Dataset df = sharedRDD.sql("select _val from Integer where _key < 9");

        // Show the result of the execution.
        df.show();

        // Close IgniteContext on all the workers.
        igniteContext.close(true);
    }
}
