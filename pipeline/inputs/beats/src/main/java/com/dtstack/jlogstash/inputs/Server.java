package com.dtstack.jlogstash.inputs;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.Future;

//import org.apache.tomcat.jni.SSL;
//import org.apache.tomcat.jni.Time;




import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.jlogstash.inputs.Beats.MessageListener;

import javax.net.ssl.SSLException;

import java.util.concurrent.TimeUnit;

/**
 * copy from https://github.com/elastic/java-lumber
 *
 */
public class Server {
    static final Logger logger = LoggerFactory.getLogger(Server.class.getName());
    static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

    private int port;
    private String host;
    private NioEventLoopGroup bossGroup;
    private NioEventLoopGroup workGroup;
    private IMessageListener messageListener;
    private SslSimpleBuilder sslBuilder;

    public Server(String host,int p,MessageListener messageListener) {
    	this.host = host;
    	this.port = p;
    	this.messageListener = messageListener;
    	this.bossGroup = new NioEventLoopGroup();
    	this.workGroup = new NioEventLoopGroup();
    }

    public void enableSSL(SslSimpleBuilder builder) {
        sslBuilder = builder;
    }

    /**
     * 开启netty server。
     * @return
     * @throws InterruptedException
     */
    public Server listen() throws InterruptedException {
        try {
            logger.info("Starting server listing port: " + this.port);

            ServerBootstrap server = new ServerBootstrap();
            server.group(bossGroup, workGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler())
                    .childHandler(new BeatsInitializer(this));

            Channel channel = server.bind(host,port).sync().channel();

            channel.closeFuture().sync();

        } finally {
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
        }

        return this;
    }

    public void stop() throws InterruptedException {
        logger.debug("Requesting the server to stop.");
        Future<?> bossWait = bossGroup.shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        Future<?> workWait = workGroup.shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        bossWait.await();
        workWait.await();
        logger.debug("Server.stopped");
    }

    public void setMessageListener(IMessageListener listener) {
        messageListener = listener;
    }

    public boolean isSslEnable() {
        if(this.sslBuilder != null) {
            return true;
        } else {
            return false;
        }
    }

    private class BeatsInitializer extends ChannelInitializer<SocketChannel> {
        private final String LOGGER_HANDLER = "logger";
        private final String SSL_HANDLER = "ssl-handler";
        private final String KEEP_ALIVE_HANDLER = "keep-alive-handler";
        private final String BEATS_PARSER = "beats-parser";
        private final String BEATS_HANDLER = "beats-handler";
        private final int DEFAULT_IDLESTATEHANDLER_THREAD = 4;
        private final EventExecutorGroup idleExecutorGroup = new DefaultEventExecutorGroup(DEFAULT_IDLESTATEHANDLER_THREAD);
        private final BeatsHandler beatsHandler;
        private final LoggingHandler loggingHandler = new LoggingHandler();
        private final Server server;

        public BeatsInitializer(Server s) {
            server = s;
            beatsHandler = new BeatsHandler(server.messageListener);
        }

        /**
         * 加入打日志、ssl、idleState、BeatsHandler和BeatsParser这几个handler。
         */
        public void initChannel(SocketChannel socket) throws SSLException {
            ChannelPipeline pipeline = socket.pipeline();

            pipeline.addLast(LOGGER_HANDLER, loggingHandler);

            if(server.isSslEnable()) {
                SslHandler sslHandler = sslBuilder.build(socket.alloc());
                pipeline.addLast(SSL_HANDLER, sslHandler);
            }

            // We have set a specific executor for the idle check, because the `beatsHandler` can be
            // blocked on the queue, this the idleStateHandler manage the `KeepAlive` signal.
            pipeline.addLast(idleExecutorGroup, KEEP_ALIVE_HANDLER, new IdleStateHandler(60*15, 5, 0));
            pipeline.addLast(BEATS_PARSER, new BeatsParser());
            pipeline.addLast(BEATS_HANDLER, this.beatsHandler);
        }
    }
}