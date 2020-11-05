package com.wei.mqtt.server.brocker.handler;

import com.wei.mqtt.server.brocker.MQTTServerHandler;
import com.wei.mqtt.server.constant.Constants;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class ConnectHandler implements MqttMessageHandler {

    private static final String NONE_ID_PREFIX = "NONE_ID_";
    private int brockerId = 1;

    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {

        MqttConnectMessage message = (MqttConnectMessage)msg;
        MqttConnAckVariableHeader variableheader = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
        MqttConnAckMessage connAckMessage = new MqttConnAckMessage(Constants.CONNACK_HEADER, variableheader);
        //ctx.write(MQEncoder.doEncode(ctx.alloc(),connAckMessage));
        ctx.write(connAckMessage);
        //String user = message.variableHeader().name();
        String stb_code = message.payload().clientIdentifier();
        log.debug("connect ,stb_code is :" + stb_code);
        String clientId = message.payload().clientIdentifier();
        if (StringUtils.isEmpty(clientId)) {
            clientId = NONE_ID_PREFIX + brockerId + System.currentTimeMillis();
        }
        Channel currentChannel = ctx.pipeline().channel();
        MQTTServerHandler.clentMap.put(clientId, currentChannel);
    }
}
