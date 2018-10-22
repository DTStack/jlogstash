package com.dtstack.jogstash.input;

import org.apache.log4j.Logger;    
import org.apache.mina.core.service.IoHandlerAdapter;    
import org.apache.mina.core.session.IoSession;    

/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2016年8月31日 下午1:17:48
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class ClientMessageHandler extends IoHandlerAdapter {    
    
    private static Logger logger = Logger.getLogger(ClientMessageHandler.class);    
    
    public void messageReceived(IoSession session, Object message)    
            throws Exception {    
        String msg = message.toString();    
        System.out.println("客户端接收到的信息为：" + msg);    
    }    
    
    @Override    
    public void exceptionCaught(IoSession session, Throwable cause)    
            throws Exception {    
    	System.out.println(cause);
        logger.info("客户端发生异常..." + cause);    
    }    
}    