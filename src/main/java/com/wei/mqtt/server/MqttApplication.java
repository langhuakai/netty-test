package com.wei.mqtt.server;

import com.wei.mqtt.server.server.MqttServer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class MqttApplication {
    public static void main(String[] args) throws Exception {
        /*try {
            int port = 8080;
            new MqttServer().start();
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        ConfigurableApplicationContext ctx = SpringApplication.run(MqttApplication.class, args);
        ctx.getBean(MqttServer.class).start();
    }
}
