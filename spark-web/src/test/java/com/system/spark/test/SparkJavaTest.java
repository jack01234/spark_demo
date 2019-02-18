package com.system.spark.test;

import com.system.spark.Application;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Arrays;

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
}
