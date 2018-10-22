package com.dtstack.jlogstash.inputs;

import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * 
 * copy from https://github.com/elastic/java-lumber
 *
 */
public class BatchEncoder extends MessageToByteEncoder<Batch> {
    private final static Logger logger = LoggerFactory.getLogger(BatchEncoder.class);


    @Override
    /**
     * 把协议、消息集大小、消息集信息存入buffer。
     */
    protected void encode(ChannelHandlerContext ctx, Batch batch, ByteBuf out) throws Exception {
        out.writeByte(batch.getProtocol());
        out.writeByte('W');
        out.writeInt(batch.size());
        out.writeBytes(getPayload(ctx, batch));
    }

    /**
     * 把batch中的message存入ctx的buffer里面。有2中version，version2是以json存储message，否则version1，按照field存储。
     * @param ctx
     * @param batch
     * @return
     * @throws IOException
     */
    protected ByteBuf getPayload(ChannelHandlerContext ctx, Batch batch) throws IOException {
        ByteBuf payload = ctx.alloc().buffer();

        // Aggregates the payload that we could decide to compress or not.
        for(Message message : batch.getMessages()) {
            if (batch.getProtocol() == Protocol.VERSION_2) {
                encodeMessageWithJson(payload, message);
            } else {
                encodeMessageWithFields(payload, message);
            }
        }
        return payload;
    }

    protected void encodeMessageWithJson(ByteBuf payload, Message message) throws JsonProcessingException {
        payload.writeByte(Protocol.VERSION_2);
        payload.writeByte('J');
        payload.writeInt(message.getSequence());

        byte[] json = JsonUtils.mapper.writeValueAsBytes(message.getData());
        payload.writeInt(json.length);
        payload.writeBytes(json);
    }

    protected void encodeMessageWithFields(ByteBuf payload, Message message) {
        payload.writeByte(Protocol.VERSION_1);
        payload.writeByte('D');
        payload.writeInt(message.getSequence());
        payload.writeInt(message.getData().size());

        for(Object entry : message.getData().entrySet()) {
            Map.Entry e = (Map.Entry) entry;
            byte[] key = ((String) e.getKey()).getBytes();
            byte[] value = ((String) e.getValue()).getBytes();

            logger.debug("New entry: key l: " + key.length  + ", value: " + value.length);

            payload.writeInt(key.length);
            payload.writeBytes(key);
            payload.writeInt(value.length);
            payload.writeBytes(value);
        }
    }
}