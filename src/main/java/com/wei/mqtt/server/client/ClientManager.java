package com.wei.mqtt.server.client;
import io.netty.channel.Channel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientManager {

    private static ClientManager instance = new ClientManager();
    private ClientManager(){}
    public static ClientManager getInstance(){
        return instance;
    }

    //IP与信道的对应关系
    private Map<String, Channel> channelMap = new ConcurrentHashMap<>();

    //添加信道
    public void putChannel(String ip, Channel channel){
        this.channelMap.put(ip, channel);
    }

    //删除信道
    public void removeChannel(String ip){
        this.channelMap.remove(ip);
    }

    //发送消息
    public void sendMsg(String ip, String msg){
        Channel channel = this.channelMap.get(ip);
        if(channel != null){
            channel.writeAndFlush(msg);
        }
    }

    //处理消息
    public void handleMsg(String ip, String msg){
        this.sendMsg(ip, msg);
    }
}
