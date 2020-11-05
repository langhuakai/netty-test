package com.wei.mqtt.server.brocker.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;

public class PubRecHandler implements MqttMessageHandler {

    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {

        // 移除消息
        MqttMessageIdVariableHeader mqttMessageIdVariableHeader = (MqttMessageIdVariableHeader) msg.variableHeader();
        int messageId = mqttMessageIdVariableHeader.messageId();
        /*if (clearSession(ctx)) {
            Session session = getSession(ctx);
            session.removePubMsg(messageId);
            session.savePubRelMsg(messageId);
        } else {
            String clientId = clientId(ctx);
            publishMessageService.remove(clientId, messageId);

            // 保存 pubRec
            pubRelMessageService.save(clientId, messageId);
        }*/

        MqttMessage mqttMessage = MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(messageId),
                null
        );
        ctx.writeAndFlush(mqttMessage);
    }
}
