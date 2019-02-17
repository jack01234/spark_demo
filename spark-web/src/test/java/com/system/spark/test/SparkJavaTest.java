package com.system.spark.test;

import com.system.spark.Application;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class SparkJavaTest {

    @Autowired
    private Ignite ignite;

    @Test
    public void test(){
        log.info("ignite {}",ignite);
    }
}
