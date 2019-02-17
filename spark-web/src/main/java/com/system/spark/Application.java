package com.system.spark;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@SpringBootApplication
public class Application {

    @RequestMapping(path = "/hello",method = RequestMethod.GET)
    public String hello(){
        log.info(">>>>>>>>>>hello world!<<<<<<<<<<<");
        return "hello world!!!";
    }
    public static void main(String[] args) {
        new SpringApplication().run(Application.class,args);
    }
}
