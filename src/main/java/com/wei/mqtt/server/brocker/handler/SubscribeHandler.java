package com.wei.mqtt.server.brocker.handler;

import com.wei.mqtt.server.brocker.MQTTServerHandler;
import com.wei.mqtt.server.constant.Constants;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.wei.mqtt.server.brocker.MQTTServerHandler.allTopics;

public class SubscribeHandler implements MqttMessageHandler {

    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {

        MqttSubscribeMessage message = (MqttSubscribeMessage)msg;
        int msgId = message.variableHeader().messageId();
        if (msgId == -1) {
            msgId = 1;
        }
        MqttMessageIdVariableHeader header = MqttMessageIdVariableHeader.from(msgId);

        MqttSubAckPayload payload = new MqttSubAckPayload(0);
        Channel currentChannel = ctx.pipeline().channel();
        List<MqttTopicSubscription> topicSubscriptions =  message.payload().topicSubscriptions();
        for (MqttTopicSubscription topicSubscription : topicSubscriptions) {
            allTopics.add(topicSubscription.topicName());
            Set<Channel> channelSet = MQTTServerHandler.topicClientMap.get(topicSubscription.topicName());
            if (channelSet == null) {
                channelSet = new HashSet<Channel>();
                channelSet.add(currentChannel);
                MQTTServerHandler.topicClientMap.put(topicSubscription.topicName(), channelSet);
            } else {
                channelSet.add(currentChannel);
            }
            //  topicClientMap.put(topicSubscription.topicName(), channelSet);
        }

        MqttSubAckMessage suback = new MqttSubAckMessage(Constants.SUBACK_HEADER, header, payload);
        ctx.write(suback);
    }
}
