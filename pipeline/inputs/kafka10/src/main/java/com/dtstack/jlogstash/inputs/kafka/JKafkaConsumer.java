/**
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
package com.dtstack.jlogstash.inputs.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JKafkaConsumer {

    private static Logger logger = LoggerFactory.getLogger(JKafkaConsumer.class);

    private volatile Properties props;

    private static List<JKafkaConsumer> jconsumerList = new CopyOnWriteArrayList<>();

    private Map<String, Client> containers = new ConcurrentHashMap<>();

    private ExecutorService executor = Executors.newCachedThreadPool();

    public JKafkaConsumer(Properties props) {
        this.props = props;
    }

    public static JKafkaConsumer init(Properties p) throws IllegalStateException {

        Properties props = new Properties();
        props.put("max.poll.interval.ms", "86400000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        if(p != null) {
            props.putAll(p);
        }

        JKafkaConsumer jKafkaConsumer = new JKafkaConsumer(props);

        jconsumerList.add(jKafkaConsumer);

        return jKafkaConsumer;
    }

    public static JKafkaConsumer init(String bootstrapServer) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);

        return init(props);
    }

    public JKafkaConsumer add(String topics, String group, Caller caller, long timeout, int threadCount) {
        Properties clientProps = new Properties();
        clientProps.putAll(props);
        clientProps.put("group.id", group);

        if (threadCount < 1) {
            threadCount = 1;
        }
        for (int i = 0; i < threadCount; i++) {
            containers.put(topics + "_" + i, new Client(clientProps, Arrays.asList(topics.split(",")), caller, timeout));
        }

        return this;
    }

    public JKafkaConsumer add(String topics, String group, Caller caller) {
        return add(topics, group, caller, Long.MAX_VALUE, 1);
    }

    public JKafkaConsumer add(String topics, String group, Caller caller, int threadCount) {
        return add(topics, group, caller, Long.MAX_VALUE, threadCount);
    }

    public void execute() {
        for (Map.Entry<String, Client> c : containers.entrySet()) {
            executor.execute(c.getValue());
        }
    }

    public interface Caller {

        public void processMessage(String message);

        public void catchException(String message, Throwable e);
    }

    public class Client implements Runnable {

        private Caller caller;

        private volatile boolean running = true;

        private long pollTimeout;

        private KafkaConsumer<String, String> consumer;

        public Client(Properties clientProps, List<String> topics, Caller caller, long pollTimeout) {

            this.pollTimeout = pollTimeout;
            this.caller = caller;

            consumer = new KafkaConsumer<>(clientProps);
            consumer.subscribe(topics);
        }

        @Override
        public void run() {

            try {

                while (running) {

                    ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
                    for (ConsumerRecord<String, String> r : records) {

                        if (r.value() == null || "".equals(r.value())) {
                            continue;
                        }

                        try {
                            caller.processMessage(r.value());

                        } catch (Throwable e) {
                            caller.catchException(r.value(), e);
                        }
                    }
                }

            } catch (WakeupException e) {
                logger.warn("WakeupException to close kafka consumer");
            } catch (Throwable e) {
                caller.catchException("", e);
            } finally {
                consumer.close();
            }
        }

        public void close() {
            try {
                running = false;
                consumer.wakeup();
            } catch(Exception e) {
                logger.error("close kafka consumer error",e);
            }
        }
    }

    public void close() {
        for (Map.Entry<String, Client> c : containers.entrySet()) {
            containers.remove(c.getKey());
            c.getValue().close();
        }
    }

    public static void closeAll() {
        for (JKafkaConsumer c : jconsumerList) {
            c.close();
        }
    }

    public static void main(String[] args) throws Exception {

        JKafkaConsumer consumer = JKafkaConsumer.init("172.16.10.86:9092");
        consumer.add("liangchentopic", "liangchen_group", new Caller() {

            @Override
            public void processMessage(String message) {
                int i = Integer.valueOf(message);
                try {
                    Thread.sleep(i*1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            @Override
            public void catchException(String message, Throwable e) {
                e.printStackTrace();
            }
        }, 1).execute();

        Thread.sleep(100000);

        consumer.close();
    }

}
