package ru.gb.storage.client;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import ru.gb.storage.commons.handler.JsonDecoder;
import ru.gb.storage.commons.handler.JsonEncoder;
import ru.gb.storage.commons.message.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.LocalDateTime;
import java.util.Date;

public class Client {

    public static void main(String[] args) {
        new Client().start();
    }

    public void start() {
        final NioEventLoopGroup group = new NioEventLoopGroup(1);
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                    new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 3, 0, 3),
                                    new LengthFieldPrepender(3),
                                    new JsonDecoder(),
                                    new JsonEncoder(),
                                    new SimpleChannelInboundHandler<Message>() {
                                        @Override
                                        public void channelActive(ChannelHandlerContext ctx) throws Exception {
                                            final FileRequestMessage message = new FileRequestMessage();
                                            message.setPath("D:\\PFRO.log");
                                            ctx.writeAndFlush(message);
                                        }

                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, Message message) throws Exception {
                                            System.out.println("receive msg " + message);
                                            if (message instanceof FileContentMessage){
                                                FileContentMessage fileContentMessage = (FileContentMessage) message;
                                                try(final RandomAccessFile accessFile = new RandomAccessFile("D:\\from.log", "rw")) {
                                                    System.out.println(fileContentMessage.getStartPosition());
                                                    accessFile.seek(fileContentMessage.getStartPosition());
                                                    accessFile.write(fileContentMessage.getContent());
                                                    if (fileContentMessage.isLast()){
                                                        ctx.close();
                                                    }
                                                }catch (IOException e){
                                                    throw new RuntimeException(e);
                                                }

                                            }
                                        }
                                    }
                            );
                        }
                    });

            System.out.println("Client started");
            Channel channel = bootstrap.connect("localhost", 9000).sync().channel();
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }
    }
}