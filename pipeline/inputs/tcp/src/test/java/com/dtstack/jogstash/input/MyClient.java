package com.dtstack.jogstash.input;
import java.net.InetSocketAddress;    
import java.nio.charset.Charset;    

import org.apache.log4j.Logger;    
import org.apache.mina.core.future.ConnectFuture;    
import org.apache.mina.core.service.IoConnector;    
import org.apache.mina.core.session.IoSession;    
import org.apache.mina.filter.codec.ProtocolCodecFilter;    
import org.apache.mina.filter.codec.textline.LineDelimiter;    
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;    
import org.apache.mina.transport.socket.nio.NioSocketConnector;    

import com.dtstack.jogstash.input.ClientMessageHandler;
    

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:17:54
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class MyClient {    
    
    private static Logger logger = Logger.getLogger(MyClient.class);    
    
    private static String HOST = "recv.log.dtstack.com";    
    
    private static int PORT = 8633;    
    
    public static void main(String[] args) {    
        // 创建一个非组设的客户端客户端     
        IoConnector connector = new NioSocketConnector();    
        // 设置链接超时时间     
        connector.setConnectTimeoutMillis(30000);    
        // 添加过滤器     
        connector.getFilterChain().addLast( // 添加消息过滤器     
                "codec",    
                // Mina自带的根据文本换行符编解码的TextLineCodec过滤器 看到\r\n就认为一个完整的消息结束了  
                new ProtocolCodecFilter(new TextLineCodecFactory(Charset    
                        .forName("UTF-8"), LineDelimiter.MAC.getValue(),    
                        LineDelimiter.MAC.getValue())));    
        // 添加业务逻辑处理器类     
        connector.setHandler(new ClientMessageHandler());    
        IoSession session = null;    
        try {    
            ConnectFuture future = connector.connect(new InetSocketAddress(    
                    HOST, PORT));    
            future.awaitUninterruptibly(); // 等待连接创建完成     
            session = future.getSession();
            long t1 = System.currentTimeMillis();
            StringBuffer sb = new StringBuffer();
            for(int i=0;i<200000;i++){
            	System.out.println(i);
            	sb.append("ysqysq nginx_ccc [0050d2bf234311e6ba8cac853da49b78 type=nginx_access_log tag=\"mylog\"] /Users/sishuyss/ysq_access/$2016-04-05T11:12:24.230148+08:00/$100.97.184.152 - - [25/May/2016:01:10:07 +0800] \"GET /index.php?disp=dynamic HTTP/1.0\" 301 278 \"http://log.dtstack.com/\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;Alibaba.Security.Heimdall.1142964)\" 121.42.0.85 - - - 0").append(LineDelimiter.UNIX.getValue());
            	if(i%200==0){
                    session.write(sb.toString()); 
                    sb =  new StringBuffer();
            	}
            }
            session.write(sb.toString()); 
            System.out.println("time:"+(System.currentTimeMillis()-t1));
        } catch (Exception e) {   
        	System.out.println(e.getCause());
            logger.info("客户端链接异常...");    
        }    
    
        session.getCloseFuture().awaitUninterruptibly();    
        logger.info("Mina要关闭了");    
        connector.dispose();    
    }    
    
}  