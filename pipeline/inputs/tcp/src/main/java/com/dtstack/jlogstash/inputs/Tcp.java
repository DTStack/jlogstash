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

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.util.Date;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.filter.ssl.KeyStoreFactory;
import org.apache.mina.filter.ssl.SslContextFactory;
import org.apache.mina.filter.ssl.SslFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.annotation.Required;

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:17:39
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
@SuppressWarnings("serial")
public class Tcp extends BaseInput {
	
	private static Logger logger = LoggerFactory.getLogger(Tcp.class);

	@Required(required = true)
	private static int port;

	private static String host = "0.0.0.0";

	private static int bufSize = 2048;

	private static int maxLineLength = 1024 * 1024;// 1M

	private static String encodiing = "UTF-8";

	private static String sslKey;

	private static String sslCert;

	private static boolean sslEnable = false;

	private static String sslKeyPassPhrase;

	private static boolean sslVerity = true;

	private IoAcceptor acceptor = null;

	private MinaBizHandler minaBizHandler = null;

	public Tcp(Map config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void prepare() {
		// TODO Auto-generated method stub
		if (acceptor == null) {
			acceptor = new NioSocketAcceptor();
		}
		if (minaBizHandler == null) {
			minaBizHandler = new MinaBizHandler(this);
		}
	}

	@Override
	public void emit() {
		// TODO Auto-generated method stub
		try {
			// ssl 认证
			if (sslEnable) {
				SslFilter sslFilter = new SslFilter(getSslContext());
				acceptor.getFilterChain().addLast("sslFilter", sslFilter);
				logger.warn("ssl authenticate is open");
			}
			LoggingFilter loggingFilter = new LoggingFilter();
			acceptor.getFilterChain().addLast("logger", loggingFilter);
			TextLineCodecFactory textLineCodecFactory = new TextLineCodecFactory(
					Charset.forName(encodiing));
			textLineCodecFactory.setDecoderMaxLineLength(maxLineLength);
			textLineCodecFactory.setEncoderMaxLineLength(maxLineLength);
			acceptor.getFilterChain().addLast("codec",
					new ProtocolCodecFilter(textLineCodecFactory));
			acceptor.setHandler(minaBizHandler);
			acceptor.getSessionConfig().setReadBufferSize(bufSize);
			acceptor.getSessionConfig().setWriteTimeout(10);
			// acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE,
			// 10);//空闲状态
			acceptor.bind(new InetSocketAddress(InetAddress.getByName(host),
					port));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
			System.exit(1);
		}
	}

	@Override
	public void release() {
		// TODO Auto-generated method stub
	}

	private SSLContext getSslContext() {
		SSLContext sslContext = null;
		try {
			File keyStoreFile = new File(sslKey);// 私钥
			File trustStoreFile = new File(sslCert);// 公钥
			if (keyStoreFile.exists() && trustStoreFile.exists()) {
				final KeyStoreFactory keyStoreFactory = new KeyStoreFactory();
				keyStoreFactory.setDataFile(keyStoreFile);
				if (StringUtils.isNotBlank(sslKeyPassPhrase)) {
					keyStoreFactory.setPassword(sslKeyPassPhrase);
				}

				final KeyStoreFactory trustStoreFactory = new KeyStoreFactory();
				trustStoreFactory.setDataFile(trustStoreFile);

				final SslContextFactory sslContextFactory = new SslContextFactory();
				final KeyStore keyStore = keyStoreFactory.newInstance();
				sslContextFactory.setKeyManagerFactoryKeyStore(keyStore);

				final KeyStore trustStore = trustStoreFactory.newInstance();
				sslContextFactory.setTrustManagerFactoryKeyStore(trustStore);
				sslContext = sslContextFactory.newInstance();
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return sslContext;
	}

	public class MinaBizHandler extends IoHandlerAdapter {

		private Tcp tcp;

		public MinaBizHandler(Tcp tcp) {
			this.tcp = tcp;
		}

		public void exceptionCaught(IoSession session, Throwable cause)
				throws Exception {
			logger.debug("exceptionCaught:{}",cause.getCause());
		}

		@Override
		public void sessionCreated(IoSession session) throws Exception {
			// TODO Auto-generated method stub
			super.sessionCreated(session);
		}

		@Override
		public void sessionOpened(IoSession session) throws Exception {
			// TODO Auto-generated method stub
			super.sessionOpened(session);
		}

		@Override
		public void sessionClosed(IoSession session) throws Exception {
			// TODO Auto-generated method stub
			super.sessionClosed(session);
			session.close(true);
		}

		@Override
		public void sessionIdle(IoSession session, IdleStatus status)
				throws Exception {
			// TODO Auto-generated method stub
			logger.info("IDLE : " + session.getIdleCount(status) + ">now : "
					+ new Date());
		}

		@Override
		public void messageReceived(IoSession session, Object message)
				throws Exception {
			// TODO Auto-generated method stub
			System.out.println(message);
			if (message != null) {
				String mes = message.toString();
				if (mes!=null&&!"".equals(mes)) {
					this.tcp.process(this.tcp.getDecoder().decode(mes));
				}
			}
		}

		@Override
		public void messageSent(IoSession session, Object message)
				throws Exception {
			// TODO Auto-generated method stub
			super.messageSent(session, message);
		}
	}
}
