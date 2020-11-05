package com.wei.mqtt.server.brocker.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.util.AttributeKey;

public class DisconnectHandler  implements MqttMessageHandler {

    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {

        /*if (clearSession(ctx)) {
            connectHandler.actionOnCleanSession(clientId(ctx));
        }*/

        // [MQTT-3.1.2-8]
        // If the Will Flag is set to 1 this indicates that, if the Connect request is accepted, a Will Message MUST be
        // stored on the Server and associated with the Network Connection. The Will Message MUST be published when the
        // Network Connection is subsequently closed unless the Will Message has been deleted by the Server on receipt of
        // a DISCONNECT Packet.
        /*Session session = (Session) ctx.channel().attr(AttributeKey.valueOf(Session.KEY)).get();
        session.clearWillMessage();*/

        ctx.close();
    }
}
