package com.wei.mqtt.server.server;

import com.wei.mqtt.server.handler.MQTTServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
public class MqttServer {

    //端口号
    @Value("${mqtt.port}")
    private int port;

    public MqttServer() {
    }
    public MqttServer(int port) {
        this.port = port;
    }

    //启动方法
    public void start() throws Exception {
        //负责接收客户端的连接的线程。线程数设置为1即可，netty处理链接事件默认为单线程，过度设置反而浪费cpu资源
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        //负责处理数据传输的工作线程。线程数默认为CPU核心数乘以2
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup);
            bootstrap.channel(NioServerSocketChannel.class);
            //在ServerChannelInitializer中初始化ChannelPipeline责任链，并添加到serverBootstrap中
            bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel channel) {
                    //添加编解码
                    channel.pipeline().addLast("decoder", new MqttDecoder());
                    channel.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
                    channel.pipeline().addLast(new IdleStateHandler(0, 0,
                            60));
                    channel.pipeline().addLast("socketHandler", new MQTTServerHandler());
                }
            });
            //标识当服务器请求处理线程全满时，用于临时存放已完成三次握手的请求的队列的最大长度
            bootstrap.option(ChannelOption.SO_BACKLOG, 1024);
            //是否启用心跳保活机制
            bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);

            //绑定端口后，开启监听
            ChannelFuture future = bootstrap.bind(port).sync();
            //等待服务监听端口关闭
            future.channel().closeFuture().sync();
        } finally {
            //释放资源
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
