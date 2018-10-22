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
package com.dtstack.jlogstash.inputs;

import io.netty.channel.ChannelHandlerContext;
import java.io.FileInputStream;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.inputs.BaseInput;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:15:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Beats extends BaseInput {

	private final static Logger logger = LoggerFactory.getLogger(Beats.class);

	@Required(required = true)
	private static int port;

	private static String host = "0.0.0.0";

	private static String sslCertificate;

	private static String noPkcs7SslKey;

	private static boolean sslEnable = false;
	
	private Server server;
	
	private MessageListener messageListener;

	@SuppressWarnings("rawtypes")
	public Beats(Map config) {
		super(config);
	}

	@Override
	public void prepare() {
		
	}

	@Override
	public void emit() {
		
		try {
			messageListener = new MessageListener(this);
			server = new Server(host, port, messageListener);
			if (sslEnable) {
				PrivateKeyConverter converter = new PrivateKeyConverter(
						noPkcs7SslKey, null);
				logger.debug("SSLCertificate: {}", sslCertificate);
				SslSimpleBuilder sslBuilder = new SslSimpleBuilder(
						new FileInputStream(sslCertificate),
						converter.convert(), null).setProtocols(
						new String[] { "TLSv1.2" }).setCertificateAuthorities(
						sslCertificate);
				server.enableSSL(sslBuilder);
			}
			server.listen();
		} catch (Exception e) {
			logger.error(e.getMessage());
			System.exit(1);
		}
	}

	@Override
	public void release() {

		try {
			if(server!=null)server.stop();
		} catch (InterruptedException e) {
			logger.error("beat server stop error: {}",e.getCause());
		}
	}

	/**
	 * 消息的监听器。
	 * @author daguan
	 *
	 */
	public class MessageListener implements IMessageListener {

        private Beats beats;
		
		public MessageListener(Beats beats) {
			this.beats = beats;
		}

		@SuppressWarnings({ "unchecked"})
		@Override
		/**
		 * 消息进来就交由beats处理。
		 */
		public void onNewMessage(ChannelHandlerContext ctx, Message message) {
			Map<String,Object> map =message.getData();
			logger.debug("message data,map={}",map);
			if (map!=null){
				this.beats.process(map);
			}
		}

		@Override
		public void onNewConnection(ChannelHandlerContext ctx) {
		}

		@Override
		public void onConnectionClose(ChannelHandlerContext ctx) {
		}

		@Override
		public void onException(ChannelHandlerContext ctx,Throwable cause) {
               logger.debug("onException:{}",cause.getCause());
		}
	}
}

