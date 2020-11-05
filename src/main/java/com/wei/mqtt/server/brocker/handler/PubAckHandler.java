package com.wei.mqtt.server.brocker.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;

public class PubAckHandler implements MqttMessageHandler {

    @Override
    public void process(ChannelHandlerContext ctx, MqttMessage msg) {

        MqttPubAckMessage message = (MqttPubAckMessage)msg;
//        log.debug(request);
        /* String user = ctx.channel().attr(USER).get();
         Map<String, UpMessage> requestMap=upMap.get(message.variableHeader().messageId());
         if(requestMap!=null&&requestMap.size()>0)
         {
             UpMessage upmessage=requestMap.get(user);
             if(upmessage!=null)
             {
                 upmessage.setStatus(Constants.SENDSUCESS);
                 requestMap.put(user, upmessage);
                 upMap.put(message.variableHeader().messageId(), requestMap);
             }
         }*/
    }
}
