package com.dtstack.jlogstash.inputs;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * copy from https://github.com/elastic/java-lumber
 *
 */
public class CompressedBatchEncoder extends BatchEncoder {
    private final static Logger logger = LoggerFactory.getLogger(BatchEncoder.class.getName());

    @Override
    protected ByteBuf getPayload(ChannelHandlerContext ctx, Batch batch) throws IOException {
        ByteBuf payload = super.getPayload(ctx, batch);

        Deflater deflater = new Deflater();
        ByteBufOutputStream output = new ByteBufOutputStream(ctx.alloc().buffer());
        DeflaterOutputStream outputDeflater = new DeflaterOutputStream(output, deflater);

        byte[] chunk = new byte[payload.readableBytes()];
        payload.readBytes(chunk);
        outputDeflater.write(chunk);
        outputDeflater.close();

        ByteBuf content = ctx.alloc().buffer();
        content.writeByte(batch.getProtocol());
        content.writeByte('C');


        content.writeInt(output.writtenBytes());
        content.writeBytes(output.buffer());

        return content;
    }
}