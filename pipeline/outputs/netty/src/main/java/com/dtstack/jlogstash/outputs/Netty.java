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
package com.dtstack.jlogstash.outputs;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dtstack.jlogstash.outputs.util.LocalIpAddressUtil;
import com.dtstack.jlogstash.outputs.util.flow.FlowControlShiper;
import com.dtstack.jlogstash.outputs.util.flow.Threshold;
import com.google.common.base.Strings;
import com.google.common.collect.Queues;
import org.codehaus.jackson.map.ObjectMapper;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.compression.ZlibEncoder;
import org.jboss.netty.handler.codec.compression.ZlibWrapper;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.annotation.Required;
import com.google.common.collect.Maps;

/**
 * netty 客户端
 * FIXME 完善对ssl支持
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年11月19日
 * Company: www.dtstack.com
 * @author xuchao
 *
 */
public class Netty extends BaseOutput{
	
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(Netty.class);
   
	@Required(required=true)
	public static int port;
	
	@Required(required=true)
    public static String host;
	
	private NettyClient client;

	/**是否开启数据压缩*/
	private static boolean openCompression = false;

	/**压缩等级*/
	private static int compressionLevel = 6;

	/**是否采集本地ip*/
	private static boolean openCollectIp = false;

	private static boolean isFlowControl = false;

	private static String flowThredshold = "1GB";

	private static ObjectMapper objectMapper = new ObjectMapper();
		
	/**输出数据格式:替换的变量${var}*/
	private static String format;
	
	private Map<String, String> replaceStrMap = null;

	private List<String> localIpList;

	private static String LOCAL_IP_KEY = "local_ip";
	
	private static String delimiter = System.getProperty("line.separator");

    private static int sendGapTime = 2 * 1000;//发送间隔时间大小

    private static int maxBufferSize = 5 * 1024;//最大缓存msg大小

    private SenderBuffer senderBuffer;

	private static int connetTimeoutMills = 30000;

    public Netty(Map config){
		super(config);
	}

	@Override
	public void prepare() {
        if(openCompression){
        	if(compressionLevel > 9 || compressionLevel < 0){
        		logger.error("compressionLevel must in 0-9...");
        		System.exit(-1);
        	}
        }
		client = new NettyClient(host, port, openCompression, isFlowControl, flowThredshold, connetTimeoutMills);
        if(openCompression){
    		client.setCompressionLevel(compressionLevel);
        }
		client.connect();
		formatStr(format);
        localIpList = LocalIpAddressUtil.resolveLocalIps();

        if(openCompression){//只有在使用压缩的情况下才需要使用本地缓存
            senderBuffer = new SenderBuffer(client, sendGapTime, maxBufferSize);
            senderBuffer.start();
        }

        logger.info("netty output client prepare success! remoteIp:{}, " +
                "port:{}, openCompression:{}, compressionLv:{}, openCollectIp:{}", new Object[]{host, port,
                openCompression, compressionLevel, openCollectIp});
    }

	@Override
	protected void emit(Map event) {
		
		try{
			String msg = "";
			collectIp(event);
			if(format != null){
				msg = replaceStr(event);
			}else{
				msg = objectMapper.writeValueAsString(event);
			}

			msg = msg + delimiter;

			if(openCompression){
                senderBuffer.pushData(msg);
            }else{
                client.write(msg + delimiter);
            }
		}catch(Exception e){
			logger.error("", e);
		}
	}

    @Override
    public void release() {
        if(senderBuffer != null){
            senderBuffer.stop();
        }
    }

    private void collectIp(Map event){
	    if(openCollectIp && !event.containsKey(LOCAL_IP_KEY)){
            event.put(LOCAL_IP_KEY, localIpList);
        }
    }
	
	private String replaceStr(Map event){
		String outStr = format;
		for(Entry<String, String> tmpEntry : replaceStrMap.entrySet()){
			String key = tmpEntry.getKey();
			String val = tmpEntry.getValue();
			String newStr = (String) event.get(val);
			if(newStr == null){
				continue;
			}
			
			outStr = outStr.replace(key, newStr);
		}
		
		return outStr;
	}
	
	private void formatStr(String format){

		if(this.format == null){
			return;
		}

		if(replaceStrMap != null){
			return;
		}

		replaceStrMap = Maps.newHashMap();
		Pattern pattern = Pattern.compile("(\\$\\{[a-z0-9A-Z._-]+\\})");
		Matcher matcher = pattern.matcher(format);
		boolean flag = false;
		while(matcher.find()){
			flag = true;
			String replaceStr = matcher.group();
			String str = replaceStr.replace("${", "").replace("}", "");
			replaceStrMap.put(replaceStr, str);
		}

		if(!flag){
			logger.error("invalid format str cannot matcher pattern:{}.", "(\\$\\{[a-z0-9A-Z._-]+\\})");
			System.exit(-1);
		}
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
	    logger.error("{}", e);
    }

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		logger.warn("channel closed.do connect after:{} seconds.", CONN_DELAY);
		//重连
		timer.newTimeout(new TimerTask() {
			
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

class FLowControlHandler implements ChannelDownstreamHandler {

	private static final Logger logger = LoggerFactory.getLogger(FLowControlHandler.class);

	private FlowControlShiper flowControlShiper;

	public FLowControlHandler(String threadHold) {
		this.flowControlShiper = new FlowControlShiper(Threshold.create(threadHold));
	}

	@Override
	public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {

		if (e instanceof MessageEvent) {

			logger.debug("flowControl,messageReceived start");

			ChannelBuffer msg = (ChannelBuffer)((MessageEvent)e).getMessage();

			if (msg != null) {
				flowControlShiper.acquire(msg.readableBytes());
				logger.debug("FLowControlHandler acquire, msg={},length={}", msg, msg.readableBytes());
			}

		}

		ctx.sendDownstream(e);
	}

	protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {

		logger.debug("flowControl,messageReceived start");

		if (msg != null) {
			flowControlShiper.acquire(msg.toString().length());
			logger.debug("FLowControlHandler acquire, msg={},length={}", msg, msg.toString().length());
		}

		return msg;
	}

}


class NettyClient{
	
	private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);
	
	private int port;
	
	private String host;

	private boolean openCompression;

	private int compressionLevel = 6;
	
	private volatile Channel channel;
	
	private volatile ClientBootstrap bootstrap;
	
	private final Timer timer = new HashedWheelTimer();
	
	public Object lock = new Object();

	private boolean isFlowControl;

	private String flowThreshold;

	private int connectTimeout;

	public NettyClient(String host, int port, boolean openCompression, boolean isFlowControl, String flowThredhold, int connectTimeout) {
		this.host = host;
		this.port = port;
		this.openCompression = openCompression;
		this.isFlowControl = isFlowControl;
		this.flowThreshold = flowThredhold;
		this.connectTimeout = connectTimeout;
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

				if(openCompression){
                    pipeline.addLast("zlibEncoder", new ZlibEncoder(ZlibWrapper.GZIP, compressionLevel));
                }

				if (isFlowControl) {
					logger.debug("flowControl,flowThreshold={}", flowThreshold);
					pipeline.addLast("flowControl", new FLowControlHandler(flowThreshold));
				}

                pipeline.addLast("encoder", new StringEncoder());
				return pipeline;
			}
		});
		
		bootstrap.setOption("remoteAddress", new InetSocketAddress(host, port));
		try {
			ChannelFuture future = bootstrap.connect().sync();
			channel = future.getChannel();
		} catch (Exception e) {
			logger.error("", e);
			bootstrap.releaseExternalResources();
			System.exit(-1);//第一次连接出现异常直接退出,不走重连
		}
	}
	
	public boolean write(String msg){
		
		boolean canWrite = channel.isConnected() && channel.isWritable();
		while(!canWrite){
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				logger.error("", e);
			}
			canWrite = channel.isConnected() && channel.isWritable();
		}
		
		channel.write(msg);
		return true;
	}

	public ClientBootstrap getBootstrap() {
		return bootstrap;
	}

	public void setChannel(ChannelFuture channelfuture) {
		this.channel = channelfuture.getChannel();
	}

    public void setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }

}

class SenderBuffer implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(SenderBuffer.class);

    private static int MAX_QUEUE_SIZE = 10000;

    private BlockingQueue<String> queue = Queues.newArrayBlockingQueue(MAX_QUEUE_SIZE);

    private int currBufferSize = 0;

    private long lastSendTime = -1;//缓存上次发送的时间

    private int sendGapTime;//发送间隔时间大小

    private int maxBufferSize;//最大缓存msg大小

    private StringBuffer sb = new StringBuffer("");

    private boolean run = false;

    private NettyClient client;

    public SenderBuffer(NettyClient nettyClient, int sendGapTime, int maxBufferSize){
        this.client = nettyClient;
        this.sendGapTime = sendGapTime;
        this.maxBufferSize = maxBufferSize;
        lastSendTime = System.currentTimeMillis();
    }

    public void start(){
        ExecutorService es = Executors.newSingleThreadExecutor();
        run = true;
        es.submit(this);
        logger.info("-----start sender buffer success------");
    }

    public void pushData(String msg){
		try {
			queue.put(msg);
		} catch (InterruptedException e) {
			logger.error("pushData err",e);
		}
	}
//
    public void stop(){
        run = false;
        //是否需要等待线程执行完成
        sendBufferMsg();
    }


    @Override
    public void run() {
        while(run){
            try{
                String data = queue.poll(sendGapTime, TimeUnit.MILLISECONDS);
                if(data == null){//超时发送缓冲区数据
                    sendBufferMsg();
                }else{
                    pushToPool(data);
                }
            }catch (Exception e){
                logger.error("", e);
            }
        }
    }

    private void pushToPool(String msg){
        sb.append(msg);
        currBufferSize += msg.length();

        if(System.currentTimeMillis() - lastSendTime >= sendGapTime){
            sendBufferMsg();
            return;
        }

        if(currBufferSize >= maxBufferSize){
            sendBufferMsg();
            return;
        }
    }

    private void sendBufferMsg(){
        String msg = sb.toString();
        currBufferSize = 0;
        lastSendTime = System.currentTimeMillis();
        sb = new StringBuffer("");

        if(!Strings.isNullOrEmpty(msg)){
            client.write(msg);
        }
    }
}
