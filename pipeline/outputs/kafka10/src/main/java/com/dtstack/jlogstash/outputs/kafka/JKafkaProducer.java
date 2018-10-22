package com.dtstack.jlogstash.outputs.kafka;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;

/**
 * Created by daguan on 18/9/3.
 */
public class JKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(JKafkaProducer.class);

    private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(100);

    private KafkaProducer<String, String> producer;

    public JKafkaProducer(Properties props) {
        producer = new KafkaProducer<>(props);
    }

    public static JKafkaProducer init(Properties p) {

        Properties props = new Properties();

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.timeout.ms", "86400000");
        props.put("retries", "1000000");
        props.put("max.in.flight.requests.per.connection", "1");

        if(p != null) {
            props.putAll(p);
        }

        return new JKafkaProducer(props);
    }

    public static JKafkaProducer init(String bootstrapServers) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        return init(props);
    }

    /**
     * 发送消息，失败重试。
     * @param topic
     * @param key
     * @param value
     */
    public void sendWithRetry(String topic, String key, String value) {
        while(!queue.isEmpty()) {
            sendWithBlock(topic, key, queue.poll());
        }

        sendWithBlock(topic, key, value);
    }


    /**
     * 发送消息，失败阻塞（放到有界阻塞队列）。
     * @param topic
     * @param key
     * @param value
     */
    public void sendWithBlock(String topic, String key, final String value) {

        if(value == null) {
            return;
        }

        producer.send(new ProducerRecord<String, String>(topic, key, value), new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                try {

                    if (exception != null) {
                        queue.put(value);
                        logger.error("send data failed, wait to retry, value={},error={}", value, exception.getMessage());
                        Thread.sleep(1000l);
                    }
                } catch (InterruptedException e) {
                    logger.error("kafka send callback error",e);
                }

            }
        });

    }

    public void close() {
        producer.close();
    }

    public void flush() {
        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("args="+JSON.toJSONString(args));
//		perf(args[0], args[1], new Integer(args[2]), new Integer(args[3]), new Integer(args[4]));
        test();
    }

    public static void test() throws InterruptedException {
        int i = 0;
        JKafkaProducer p = JKafkaProducer.init("localhost:9092");

        while (i++ < 100000) {
            p.sendWithRetry("dt_all_log", "a" + System.currentTimeMillis(),
                    i+"");
			System.out.println("ddd");
			Thread.sleep(1000l);
        }

        p.close();
    }


}

