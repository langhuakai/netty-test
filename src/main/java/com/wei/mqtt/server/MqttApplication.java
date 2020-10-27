package com.wei.mqtt.server;

import com.wei.mqtt.server.server.MqttServer;

public class MqttApplication {
    public static void main(String[] args) {
        try {
            int port = 8080;
            new MqttServer(port).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
