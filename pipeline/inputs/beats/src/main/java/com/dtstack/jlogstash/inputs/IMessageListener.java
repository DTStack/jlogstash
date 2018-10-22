package com.dtstack.jlogstash.inputs;

import io.netty.channel.ChannelHandlerContext;

/**
 * copy from https://github.com/elastic/java-lumber
 *
 */
public interface IMessageListener {
    public void onNewMessage(ChannelHandlerContext ctx, Message message);
    public void onNewConnection(ChannelHandlerContext ctx);
    public void onConnectionClose(ChannelHandlerContext ctx);
    public void onException(ChannelHandlerContext ctx,Throwable cause);
}