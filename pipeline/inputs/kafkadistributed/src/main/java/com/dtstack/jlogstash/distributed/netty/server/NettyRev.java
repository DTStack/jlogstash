/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.jlogstash.distributed.netty.server;

import com.dtstack.jlogstash.distributed.logmerge.LogPool;

import org.apache.commons.lang3.StringUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * netty 接收路由的数据
 * Date: 2017/1/3
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */

public class NettyRev {

    private static Logger logger = LoggerFactory.getLogger(NettyRev.class);

    private static int port;

    private static String host = "0.0.0.0";

    private static String encoding = "utf-8";

    private static int receiveBufferSize = 1024 * 1024 * 20;// 设置缓存区大小20M

    private static String delimiter = System.getProperty("line.separator");

    private static String multilineDelimiter = (char)29 +"";

    private ServerBootstrap bootstrap;

    private Executor bossExecutor;

    private Executor workerExecutor;

    public NettyRev(int port){
    	NettyRev.port = port;
    }
    
    public NettyRev(String localAddress){
    	String[] ls = localAddress.split(":");
    	NettyRev.port = Integer.parseInt(ls[1]);
    }
    
    
    public void startup(){
        try {
            bossExecutor = Executors.newCachedThreadPool();
            workerExecutor = Executors.newCachedThreadPool();
            bootstrap = new ServerBootstrap(
                    new NioServerSocketChannelFactory(
                            bossExecutor,workerExecutor));
            final NettyServerHandler nettyServerHandler = new NettyServerHandler();
            // 设置一个处理客户端消息和各种消息事件的类(Handler)
            bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                @Override
                public ChannelPipeline getPipeline() throws Exception {
                    ChannelPipeline pipeline = Channels.pipeline();
                    pipeline.addLast(
                            "decoder",
                            new DelimiterBasedFrameDecoder(Integer.MAX_VALUE,
                                    false, true, ChannelBuffers.copiedBuffer(
                                    delimiter,
                                    Charset.forName(encoding))));
                    pipeline.addLast("handler", nettyServerHandler);
                    return pipeline;
                }
            });
            bootstrap.setOption("child.receiveBufferSize", receiveBufferSize);
            bootstrap.setOption("child.keepAlive", true);
//            bootstrap.setOption("child.tcpNoDelay", true);
            bootstrap.bind(new InetSocketAddress(InetAddress.getByName(host),
                    port));
            logger.warn("netty server start up success port:{}.", port);
        } catch (Exception e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
    }


    private static class NettyServerHandler extends SimpleChannelHandler {

        public NettyServerHandler() {
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
                throws Exception {
            Object message = e.getMessage();
            if (message != null && message instanceof ChannelBuffer) {
                String mes = ((ChannelBuffer) message).toString(Charset.forName(encoding));
                if(!StringUtils.isNotBlank(mes)){
                    return;
                }
                //将数据加入到merge队列里面
                mes = mes.replaceAll(multilineDelimiter, delimiter);
                LogPool.getInstance().addLog(mes);
            }
        }

        @Override
        public void exceptionCaught(
                ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
            logger.debug("netty io error:", e.getCause());
            ctx.sendUpstream(e);
        }

    }

    public void release() {
        // TODO Auto-generated method stub
        if(bootstrap!=null)bootstrap.shutdown();
    }
}
