package com.wei.mqtt.server.brocker.handler;

import com.wei.mqtt.server.brocker.MQTTServerHandler;
import com.wei.mqtt.server.constant.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

@Slf4j
public class PublishHandler implements MqttMessageHandler{

    private final AttributeKey<String> USER = AttributeKey.valueOf("user");

    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {
        MqttPublishMessage message = (MqttPublishMessage)msg;
        ByteBuf buf = message.payload();
        String msgs = new String(ByteBufUtil.getBytes(buf));
        log.debug("终端消息上报 start，终端编码为："+ctx.channel().attr(USER).get()+" 终端上报消息体："+msgs);
        int msgId = message.variableHeader().packetId();
        if (msgId == -1) {
            msgId = 1;
        }
        //主题名
        String topicName = message.variableHeader().topicName();
        Set<Channel> clientSet = MQTTServerHandler.topicClientMap.get(topicName);
        MQTTServerHandler.messageId = MQTTServerHandler.messageId + 1;
        MqttPublishMessage mpm = MqttMessageBuilders.publish()
                .messageId(MQTTServerHandler.messageId)
                .qos(MqttQoS.AT_MOST_ONCE)
                .topicName(topicName)
                .retained(false)
                .payload(Unpooled.wrappedBuffer(message.payload()))
                .build();
        for (Channel channel : clientSet) {
            channel.writeAndFlush(mpm);
        }
        if (message.fixedHeader().qosLevel() == MqttQoS.AT_LEAST_ONCE)
        {
            MqttMessageIdVariableHeader header = MqttMessageIdVariableHeader.from(msgId);
            MqttPubAckMessage puback = new MqttPubAckMessage(Constants.PUBACK_HEADER, header);
            ctx.write(puback);
        }
        msgs = null;
        topicName = null;
    }
}
