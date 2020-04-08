package com.ifeng.rocketmq.connect;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

/**
 * @Des:
 * @Author: jiangchuan
 * <p>
 * @Date: 20-4-7
 */
@SpringBootApplication()
@ImportResource("classpath:applicationContext.xml")
public class ConnectStarter {

    public static void main(String[] args) throws Exception{
        SpringApplication springApplication = new SpringApplication(ConnectStarter.class);
        springApplication.setBannerMode(Banner.Mode.OFF);
        springApplication.run(args);
    }
}
