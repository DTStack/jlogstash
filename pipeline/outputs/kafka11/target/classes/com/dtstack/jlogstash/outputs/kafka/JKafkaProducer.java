package com.dtstack.jlogstash.outputs.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: haisi
 * @date 2019-06-25 11:07
 */
public class JKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(JKafkaProducer.class);

    private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(100);

    private KafkaProducer<String,String> producer;

    public JKafkaProducer(Properties props) {
        producer = new KafkaProducer<>(props);
    }

    public static JKafkaProducer init(Properties p) {

        Properties props = new Properties();

//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("request.timeout.ms", "86400000");
//        props.put("retries", "1000000");
//        props.put("max.in.flight.requests.per.connection", "1");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,86400000);
        props.put(ProducerConfig.RETRIES_CONFIG,1000000);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,1);

        if (p!=null){
            props.putAll(p);
        }

        return new JKafkaProducer(props);
    }

    public static JKafkaProducer init(String bootstrapServers) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        return init(props);
    }

    public void close(){
        producer.close();
    }

    public void flush() {
        producer.flush();
    }

    public void sendWithRetry(String topic, String key, String value) {
        while (!queue.isEmpty()){
            sendWithBlock(topic,key,queue.poll());
        }
        sendWithBlock(topic, key, value);
    }

    public void sendWithBlock(String topic, String key, final String value){

        if (value==null){
            return;
        }
        /*
         * MARK
         */
        producer.send(new ProducerRecord<>(topic, key, value), (RecordMetadata metadata, Exception exception) -> {
            try{
                if (exception != null){
                    queue.put(value);
                    logger.error("send data failed, wait to retry, value={},error={}", value, exception.getMessage());
                    Thread.sleep(1000L);
                }
            } catch (InterruptedException e){
                logger.error("kafka11 send callback error",e);
            }
        });

    }

    public static void main(String[] args) throws InterruptedException {
        test();
    }

    private static void test() throws InterruptedException {
        int i=0,n=100000;
        JKafkaProducer p = JKafkaProducer.init("localhost:9092");

        while (i++ < n){
            p.sendWithRetry("dt_all_log", "a" + System.currentTimeMillis(),
                    i+"");
            System.out.println("123");
            Thread.sleep(1000L);
        }
        p.close();
    }
}
