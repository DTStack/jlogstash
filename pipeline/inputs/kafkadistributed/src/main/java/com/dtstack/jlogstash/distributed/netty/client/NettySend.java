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
package com.dtstack.jlogstash.distributed.netty.client;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.exception.ExceptionUtil;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年12月30日 下午1:16:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class NettySend{
	
	private static final Logger logger = LoggerFactory.getLogger(NettySend.class);
   
	private  int port;
	
	private  String host;
	
	private NettyClient client;
	
	private static ObjectMapper objectMapper = new ObjectMapper();

		
	public  NettySend(String broker) {
		String[] bs = broker.split(":");
		this.host = bs[0];
		this.port = Integer.parseInt(bs[1]);
		client = new NettyClient(this.host, this.port);
		client.connect();
	}

	public boolean emit(Map event) {
		try{
			String msg = objectMapper.writeValueAsString(event);
			return client.write(msg);
		}catch(Exception e){
			logger.error(ExceptionUtil.getErrorMessage(e));
		}
		logger.error("{}:netty client emit error",this.host);
		return false;
	}


	public void release(){
		this.client.getBootstrap().shutdown();
	}
}

class NettyClientHandler extends SimpleChannelHandler {
	
	private static final int CONN_DELAY = 3;
	
	private NettyClient client;
	
	final Timer timer;
	
	public NettyClientHandler(NettyClient client, Timer timer){
		this.client = client;
		this.timer = timer;
	}
	
	private static final Logger logger = LoggerFactory.getLogger(NettyClientHandler.class);
	
	@Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
	    logger.error(ExceptionUtil.getErrorMessage(e.getCause()));    
    }

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		logger.warn("channel closed.do connect after:{} seconds.", CONN_DELAY);
		//重连
		timer.newTimeout(	new TimerTask() {
			
			@Override
			public void run(Timeout timeout) throws Exception {
				ChannelFuture channelfuture = client.getBootstrap().connect();
				client.setChannel(channelfuture);
			}
		}, CONN_DELAY, TimeUnit.SECONDS);
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		logger.warn("connect to:{} success.", getRemoteAddress());
	}	
	
	InetSocketAddress getRemoteAddress() {
		return (InetSocketAddress) client.getBootstrap().getOption("remoteAddress");
	}
	
}

class NettyClient{
	
	private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);
	
	private int port;
	
	private String host;
	
	private volatile Channel channel;
	
	private volatile ClientBootstrap bootstrap;
	
	private final Timer timer = new HashedWheelTimer();

    private static String multilineDelimiter = (char)29 +"";
    
    private static String delimiter = System.getProperty("line.separator");

				
	public NettyClient(String host, int port){
		this.host = host;
		this.port = port;
	} 
	
	public void connect(){

		bootstrap = new ClientBootstrap( new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));
		bootstrap.setOption("tcpNoDelay", false);
		bootstrap.setOption("keepAlive", true);

		final NettyClientHandler handler = new NettyClientHandler(this, timer);
		
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline =  Channels.pipeline();
				pipeline.addLast("handler", handler);
				pipeline.addLast("encoder", new StringEncoder());
				return pipeline;
			}
		});
		
		bootstrap.setOption("remoteAddress", new InetSocketAddress(host, port));
		try {
			ChannelFuture future = bootstrap.connect().sync();
			channel = future.getChannel();
		} catch (Exception e) {
			logger.error(ExceptionUtil.getErrorMessage(e));
			bootstrap.releaseExternalResources();
			System.exit(-1);//第一次连接出现异常直接退出,不走重连
		}
	}
	
	public boolean write(String msg){
		boolean canWrite = channel.isConnected() && channel.isWritable();
		int index = 0;
		while(!canWrite){
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				logger.error(ExceptionUtil.getErrorMessage(e));
			}
			canWrite = channel.isConnected() && channel.isWritable();
			if(++index > 3)return false;
		}
		channel.write(msg.replaceAll(delimiter, multilineDelimiter)+delimiter);
		return true;
	}

	public ClientBootstrap getBootstrap() {
		return bootstrap;
	}

	public void setChannel(ChannelFuture channelfuture) {
		this.channel = channelfuture.getChannel();
	}
}
