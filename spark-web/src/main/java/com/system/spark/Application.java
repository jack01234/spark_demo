package com.system.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignition;
import org.apache.ignite.spark.IgniteDataFrameSettings;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

@Slf4j
@RestController
@SpringBootApplication
public class Application {
    private static final String CONFIG = "example-default.xml";

    @RequestMapping(path = "/hello",method = RequestMethod.GET)
    public String hello() {
        log.info(">>>>>>>>>>hello world!<<<<<<<<<<<");
        return "hello world!!!";
    }
    public static void main(String[] args) {
        new SpringApplication().run(Application.class,args);
    }
}
