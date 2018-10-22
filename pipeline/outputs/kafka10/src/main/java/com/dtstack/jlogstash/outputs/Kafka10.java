package com.dtstack.jlogstash.outputs;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import com.dtstack.jlogstash.outputs.kafka.JKafkaProducer;
import com.dtstack.jlogstash.outputs.kafka.MonitorCluster;
import com.dtstack.jlogstash.outputs.kafka.NamedThreadFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.render.Formatter;

public class Kafka10 extends BaseOutput {

	private static final Logger logger = LoggerFactory.getLogger(Kafka10.class);

	private static ObjectMapper objectMapper = new ObjectMapper();

	private Properties props;

	private static String timezone;

	@Required(required = true)
	private static String topic;

	private static Map<String, Map<String, String>> topicSelect;

	private ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("kafka-monitor"));

	private Set<Map.Entry<String, Map<String, String>>> entryTopicSelect;

	@Required(required = true)
	private static String bootstrapServers;

	private static Map<String, String> producerSettings;

	private static String zkAddress;

	private static int monitorZkHealthIntervalMs = 5 * 1000;

	private static AtomicBoolean isKafkaHealth = new AtomicBoolean(true);

	private static AtomicBoolean isInit = new AtomicBoolean(false);

	private static JKafkaProducer producer;

	@SuppressWarnings("rawtypes")
	public Kafka10(Map config) {
		super(config);
	}

	public void prepare() {

		try {

			if (topicSelect != null) {
				entryTopicSelect = topicSelect.entrySet();
			}

			if (props == null) {
				props = new Properties();
			}
			if (producerSettings != null) {
				props.putAll(producerSettings);
			}
			props.put("bootstrap.servers", bootstrapServers);

			if (isInit.compareAndSet(false, true)) {
				
				producer = JKafkaProducer.init(props);

				scheduler.scheduleWithFixedDelay(new MonitorCluster(zkAddress, isKafkaHealth), monitorZkHealthIntervalMs, monitorZkHealthIntervalMs, TimeUnit.MILLISECONDS);
				
			}

		} catch (Exception e) {
			logger.error("kafka producer init error", e);
			System.exit(1);
		}
	}

	@SuppressWarnings({ "rawtypes" })
	protected void emit(Map event) {
		try {

			while (isKafkaHealth.get() == false) {
				Thread.sleep(4000l);
				logger.warn("kafka is unhealthy, producing is stopped");
			}

			String tp = null;
			if (entryTopicSelect != null) {
				for (Map.Entry<String, Map<String, String>> entry : entryTopicSelect) {
					String key = entry.getKey();
					Map<String, String> value = entry.getValue();
					Set<Map.Entry<String, String>> sets = value.entrySet();
					for (Map.Entry<String, String> ey : sets) {
						if (ey.getKey().equals(event.get(key))) {
							tp = Formatter.format(event, ey.getValue(), timezone);
							break;
						}
					}
				}
			}
			if (tp == null) {
				tp = Formatter.format(event, topic, timezone);
			}

			producer.sendWithRetry(tp, event.toString(), objectMapper.writeValueAsString(event));
		} catch (Exception e) {
			logger.error("kafka output error to block", e);
			try {
				Thread.sleep(Long.MAX_VALUE);
			} catch (InterruptedException e1) {

			}
		}
	}

	@Override
	public void release() {
		producer.close();
		logger.warn("output kafka release.");
	}

	public static void main(String[] args) {

	}
}
