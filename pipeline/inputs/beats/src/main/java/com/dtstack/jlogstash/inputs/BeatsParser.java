package com.dtstack.jlogstash.inputs;


import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.InflaterInputStream;

/**
 * copy from https://github.com/elastic/java-lumber
 * modify @xuchao
 */
public class BeatsParser extends ByteToMessageDecoder {
    private static final int CHUNK_SIZE = 1024;
    private final static Logger logger = LoggerFactory.getLogger(BeatsParser.class);
    private static final int LIMIT_PACKAGE_SIZE = 10 * 1024 * 1024;//限制传输的数据包大小10M

    private Batch batch = new Batch();

    private enum States {
        READ_HEADER,
        READ_FRAME_TYPE,
        READ_WINDOW_SIZE,
        READ_JSON_HEADER,
        READ_COMPRESSED_FRAME_HEADER,
        READ_COMPRESSED_FRAME,
        READ_JSON,
        READ_DATA_FIELDS,
    }

    private States currentState = States.READ_HEADER;
    private long requiredBytes = 0;
    private int sequence = 0;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    	
    	in.markReaderIndex();//标记,如果出现半包需要重置
    	
        if(!hasEnoughBytes(in)) {
            return;
        }
        
        switch (currentState) {
        	//判断版本，并转由READ_FRAME_TYPE处理。
            case READ_HEADER: {
                logger.debug("Running: READ_HEADER");
                
                byte currentVersion = in.readByte();

                if(Protocol.isVersion2(currentVersion)) {
                    logger.debug("Frame version 2 detected");
                    batch.setProtocol(Protocol.VERSION_2);
                } else {
                    logger.debug("Frame version 1 detected");
                    batch.setProtocol(Protocol.VERSION_1);
                }

                transition(States.READ_FRAME_TYPE, 1);
                break;
            }
            //判断框架类型，并交由对应框架类型处理。
            case READ_FRAME_TYPE: {
                logger.debug("Running: READ_FRAME_TYPE");
                
                byte frameType = in.readByte();

                switch(frameType) {
                    case Protocol.CODE_WINDOW_SIZE: {
                        transition(States.READ_WINDOW_SIZE, 4);
                        break;
                    }
                    case Protocol.CODE_JSON_FRAME: {
                        // Reading Sequence + size of the payload
                        transition(States.READ_JSON_HEADER, 8);
                        break;
                    }
                    case Protocol.CODE_COMPRESSED_FRAME: {
                        transition(States.READ_COMPRESSED_FRAME_HEADER, 4);
                        break;
                    }
                    case Protocol.CODE_FRAME: {
                        transition(States.READ_DATA_FIELDS, 8);
                        break;
                    }
                }
                break;
            }
            //给batch设置windowSize。
            case READ_WINDOW_SIZE: {
                logger.debug("Running: READ_WINDOW_SIZE");
                this.batch.setWindowSize((int) in.readUnsignedInt());

                // This is unlikely to happen but I have no way to known when a frame is
                // actually completely done other than checking the windows and the sequence number,
                // If the FSM read a new window and I have still
                // events buffered I should send the current batch down to the next handler.
                if(!this.batch.isEmpty()) {
                    logger.warn("New window size received but the current batch was not complete, sending the current batch");
                    out.add(this.batch);
                    this.batchComplete();
                }

                transitionToReadHeader();
                break;
            }
            //版本1中数据按照字段划分。
            case READ_DATA_FIELDS: {
                // Lumberjack version 1 protocol, which use the Key:Value format.
                logger.debug("Running: READ_DATA_FIELDS");
                                
                this.sequence = (int) in.readUnsignedInt();
                int fieldsCount = (int) in.readUnsignedInt();
                int count = 0;

                Map dataMap = new HashMap<String, String>();

                while(count < fieldsCount) {
                    int fieldLength = (int) in.readUnsignedInt();
                    
                    if(checkInvalidPackage(fieldLength)){
                    	in.clear();
                    	ctx.close();
                    	return;
                    }
                    
                    if(!checkEnoughBytes(in, fieldLength, true)){
                    	return;
                    }
                    
                    String field = in.readSlice(fieldLength).toString(Charset.forName("UTF8"));

                    int dataLength = (int) in.readUnsignedInt();
                    
                    if(checkInvalidPackage(dataLength)){
                    	in.clear();
                    	ctx.close();
                    	return;
                    }
                    
                    if(!checkEnoughBytes(in, dataLength, true)){
                    	return;
                    }

                    ByteBuf buf = in.readBytes(dataLength);
                    String data = buf.toString(Charset.forName("UTF8"));

                    //避免内存泄漏：http://netty.io/wiki/reference-counted-objects.html
                    buf.release();

                    dataMap.put(field, data);

                    count++;
                }

                Message message = new Message(sequence, dataMap);
                this.batch.addMessage(message);

                if(this.batch.size() == this.batch.getWindowSize()) {
                    out.add(batch);
                    this.batchComplete();
                }

                transitionToReadHeader();

                break;
            }
            //读出json的sequence和size，并跳转READ_JSON处理。
            case READ_JSON_HEADER: {
                logger.debug("Running: READ_JSON_HEADER");

                this.sequence = (int) in.readUnsignedInt();
                int jsonPayloadSize = (int) in.readUnsignedInt();

                transition(States.READ_JSON, jsonPayloadSize);
                break;
            }
            case READ_COMPRESSED_FRAME_HEADER: {
                logger.debug("Running: READ_COMPRESSED_FRAME_HEADER");

                transition(States.READ_COMPRESSED_FRAME, in.readUnsignedInt());
                break;
            }
            
            //解压。
            case READ_COMPRESSED_FRAME: {
                logger.debug("Running: READ_COMPRESSED_FRAME");


                byte[] bytes = new byte[(int) this.requiredBytes];
                in.readBytes(bytes);

                InputStream inflater = new InflaterInputStream(new ByteArrayInputStream(bytes));
                ByteArrayOutputStream decompressed = new ByteArrayOutputStream();

                byte[] chunk = new byte[CHUNK_SIZE];
                int length = 0;

                while ((length = inflater.read(chunk)) > 0) {
                    decompressed.write(chunk, 0, length);
                }

                inflater.close();
                decompressed.close();

                transitionToReadHeader();
                ByteBuf newInput = Unpooled.wrappedBuffer(decompressed.toByteArray());
                while(newInput.readableBytes() > 0) {
                    decode(ctx, newInput, out);
                }

                break;
            }
            //json反序列化，存入batch，如果batch大小等于windowSize则输出。
            case READ_JSON: {
                logger.debug("Running: READ_JSON");

                ByteBuf buffer = in.readBytes((int) this.requiredBytes);
                byte[] arr;
                if(!buffer.hasArray()){//FIXME
                    int len =  buffer.readableBytes();
                    arr = new byte[len];
                    buffer.getBytes(0, arr);
                }else{
                    arr = buffer.array();
                }

                //避免内存泄漏：http://netty.io/wiki/reference-counted-objects.html
                buffer.release();
                
                logger.debug("before json parsed, message={}",new String(arr,"utf-8"));
                Message message = new Message(sequence, (Map) JsonUtils.mapper.readValue(arr, Object.class));
                logger.debug("after json parsed, message={}",(Map) JsonUtils.mapper.readValue(arr, Object.class));

                this.batch.addMessage(message);

                if(this.batch.size() == this.batch.getWindowSize()) {
                    out.add(this.batch);
                    this.batchComplete();
                }

                transitionToReadHeader();
                break;
            }
        }
    }

    private boolean hasEnoughBytes(ByteBuf in) {
        if(in.readableBytes() >= this.requiredBytes) {
            return true;
        }
        return false;
    }
    
    /**
     * 判断是否有足够的可读取字节
     * @param in
     * @param needLength
     * @param needReset 是否需要重置ByteBuf
     * @return
     */
    private boolean checkEnoughBytes(ByteBuf in, int needLength, boolean needReset){
    	if(in.readableBytes() < needLength){//读取数据不足等待下次tcp传输
    		if(needReset) in.resetReaderIndex();
         	return false;
        }
    	 
    	return true;
    }
    
    /**
     * 判断用户发送的数据部分是否超过限制大小
     * @param needLength
     * @return
     */
    private boolean checkInvalidPackage(int needLength){
    	if(needLength >= LIMIT_PACKAGE_SIZE){
    		logger.error("invalid msg length:{}, bigger then limit:{}.", needLength, LIMIT_PACKAGE_SIZE);
    		return true;
    	}
    	
    	return false;
    }

    public void transitionToReadHeader() {
        transition(States.READ_HEADER, 1);
    }

    public void transition(States next, long need) {
        logger.debug("Transition, from: " + this.currentState + " to: " + next + " required bytes: " + need);
        this.currentState = next;
        this.requiredBytes = need;
    }

    public void batchComplete() {
        this.requiredBytes = 0;
        this.sequence = 0;
        this.batch = new Batch();
    }
}