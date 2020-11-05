/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.wei.mqtt.server.brocker;

import com.wei.mqtt.server.brocker.handler.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * @description mqtt消息处理实现类
 * @author binggu
 * @date 2017-03-03
 */
@Sharable
@Slf4j
public class MQTTServerHandler extends SimpleChannelInboundHandler<Object> {
    
   // public static Logger log = LogManager.getLogger(MQTTServerHandler.class);
    
    private final AttributeKey<String> USER = AttributeKey.valueOf("user");
    
    public static Map<String,Long> unconnectMap=new HashMap<String, Long>();
    
    // 所有该上报的消息集合   mac+plan
    //    public static Map<Integer,Map<String, UpMessage>> upMap=new ConcurrentHashMap<Integer,Map<String, UpMessage>>();
    
    //用户数据缓存。<机顶盒号，ctx>
    public static Map<String, ChannelHandlerContext> userMap = new ConcurrentHashMap<String, ChannelHandlerContext>();
    //记载在线用户登入时间
    public static Map<String, String> userOnlineMap = new ConcurrentHashMap<String, String>();

    private static final String NONE_ID_PREFIX = "NONE_ID_";
    private int brockerId = 1;
    // 客户端map
    public static Map<String, Channel> clentMap = new HashMap<>();
    // topic列表
    public static Set<String> allTopics = new HashSet<>();
    // topic对应的channel列表
    public static Map<String, Set<Channel>> topicClientMap = new HashMap<>();
    // messageId
    public static int messageId = 1;

    public static final ChannelGroup CHANNELS = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    // 处理器map
    private static Map<MqttMessageType, MqttMessageHandler> handlerMap = new HashMap();

    static {
        handlerMap.put(MqttMessageType.CONNECT, new ConnectHandler());
        handlerMap.put(MqttMessageType.PUBLISH, new PublishHandler());
        handlerMap.put(MqttMessageType.DISCONNECT, new DisconnectHandler());
        handlerMap.put(MqttMessageType.PINGREQ, new PingReqHandler());
        handlerMap.put(MqttMessageType.PUBACK, new PubAckHandler());
        handlerMap.put(MqttMessageType.PUBCOMP, new PubCompHandler());
        handlerMap.put(MqttMessageType.PUBREC, new PubRecHandler());
        handlerMap.put(MqttMessageType.PUBREL, new PubRelHandler());
        handlerMap.put(MqttMessageType.SUBSCRIBE, new SubscribeHandler());
        handlerMap.put(MqttMessageType.UNSUBSCRIBE, new UnsubscribeHandler());
    }

    //连接成功后调用的方法
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Send greeting for a new connection
        System.out.println("Welcome to " + InetAddress.getLocalHost().getHostName());
        ctx.writeAndFlush("Welcome to " + InetAddress.getLocalHost().getHostName() + "!\r\n");
        ctx.writeAndFlush("It is " + new Date() + " now.\r\n");
    }
    
    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object request) throws Exception {
        MqttMessage mqttMessage = (MqttMessage)request;
        MqttMessageType mqttMessageType = mqttMessage.fixedHeader().messageType();

        // 连接校验
        /*if (mqttMessageType != MqttMessageType.CONNECT &&
                ctx.channel().attr(AttributeKey.valueOf(Session.KEY)).get() == null) {
            throw new AuthorizationException("access denied");
        }*/

        Optional.of(handlerMap.get(mqttMessageType))
                .ifPresent(messageHandler -> messageHandler.process(ctx, mqttMessage));
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.debug(ctx.channel().remoteAddress().toString().substring(1,ctx.channel().remoteAddress().toString().lastIndexOf(":")) + "is close!");
        //清理用户缓存
        if (ctx.channel().hasAttr(USER))
        {
            String user = ctx.channel().attr(USER).get();
            userMap.remove(user);
            userOnlineMap.remove(user);
        }
    }
    
    /**
     * 超时处理
     * 服务器端 设置超时 ALL_IDLE  <  READER_IDLE ， ALL_IDLE 触发时发送心跳，客户端需响应，
     * 如果客户端没有响应 说明 掉线了 ，然后触发 READER_IDLE ，
     * READER_IDLE 里 关闭链接
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent)evt;
            if (event.state().equals(IdleState.READER_IDLE)) {
            	if (ctx.channel().hasAttr(USER)){
            		String user = ctx.channel().attr(USER).get();
            		 log.debug("ctx heartbeat timeout,close!"+user);//+ctx);
            		 log.debug("ctx heartbeat timeout,close!");//+ctx);
                     if(unconnectMap.containsKey(user)) {
                     	unconnectMap.put(user, unconnectMap.get(user)+1);
                     }else {
                     	unconnectMap.put(user, new Long(1));
                     }
            	}
               
                ctx.fireChannelInactive();
                ctx.close();
            } else if (event.state().equals(IdleState.ALL_IDLE)) {
            	log.debug("发送心跳给客户端！");
            	buildHearBeat(ctx);
            }
        }
        super.userEventTriggered(ctx, evt);
    }

    
    /**
     * 封装心跳请求
     * @param ctx
     */
        private void buildHearBeat(ChannelHandlerContext ctx) {
            MqttFixedHeader mqttFixedHeader=new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0);
            MqttMessage message=new MqttMessage(mqttFixedHeader);
            ctx.writeAndFlush(message);
        }
    /**
     * 封装发布
     * @param str
     * @param topicName
     * @return
     */
    public static MqttPublishMessage buildPublish(String str, String topicName, Integer messageId) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, str.length());
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(topicName, messageId);//("MQIsdp",3,false,false,false,0,false,false,60);
        ByteBuf payload = Unpooled.wrappedBuffer(str.getBytes(CharsetUtil.UTF_8));
        MqttPublishMessage msg = new MqttPublishMessage(mqttFixedHeader, variableHeader, payload);
        return msg;
    }

    
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
