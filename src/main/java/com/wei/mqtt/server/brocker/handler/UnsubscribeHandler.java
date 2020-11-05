package com.wei.mqtt.server.brocker.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;

import java.util.List;

public class UnsubscribeHandler implements MqttMessageHandler {

    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {

        MqttUnsubscribeMessage mqttUnsubscribeMessage = (MqttUnsubscribeMessage) msg;
        int messageId = mqttUnsubscribeMessage.variableHeader().messageId();
        MqttUnsubscribePayload payload = mqttUnsubscribeMessage.payload();

        // 系统主题
        List<String> collect = payload.topics();
        /*if (enableSysTopic) {
            collect = unsubscribeSysTopics(payload.topics(), ctx.channel());
        }*/

        // 非系统主题
        /*subscriptionService.unsubscribe(clientId(ctx), collect);*/

        // response
        MqttMessage mqttMessage = MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(messageId),
                null
        );
        ctx.writeAndFlush(mqttMessage);
    }
}
