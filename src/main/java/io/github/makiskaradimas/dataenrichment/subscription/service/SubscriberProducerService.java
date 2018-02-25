package io.github.makiskaradimas.dataenrichment.subscription.service;

import com.google.common.io.Resources;
import io.github.makiskaradimas.dataenrichment.config.TopicsConfig;
import io.github.makiskaradimas.dataenrichment.subscription.SubscriberValue;
import kafka.admin.TopicCommand;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author Makis Karadimas
 */
@Component
public class SubscriberProducerService {

    private KafkaProducer<Bytes, SubscriberValue> producer;
    private boolean ready = false;

    private final SubscriberConfigurationService subscriberConfigurationService;

    @Autowired
    public SubscriberProducerService(SubscriberConfigurationService subscriberConfigurationService) {
        this.subscriberConfigurationService = subscriberConfigurationService;
    }

    @PostConstruct
    private void init() {
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);

            ZkClient zkClient = new ZkClient(properties.get(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG).toString(), 20000, 20000, new ZkSerializer() {
                @Override
                public byte[] serialize(Object data) throws ZkMarshallingError {
                    try {
                        return ((String) data).getBytes("UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public Object deserialize(byte[] bytes) throws ZkMarshallingError {
                    if (bytes == null)
                        return null;
                    try {
                        return new String(bytes, "UTF-8");
                    } catch (UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            final ZkConnection zkConnection = new ZkConnection(properties.get(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG).toString(), 20000);
            final ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
            Set<Integer> partitions = new HashSet<>();
            for (SubscriberValue subscriberValue : subscriberConfigurationService.getSubscribers().values()) {
                partitions.add(subscriberValue.getPartition());
            }
            try {
                TopicCommand.createTopic(zkUtils,
                        new TopicCommand.TopicCommandOptions(new String[]{
                                "--zookeeper", properties.get(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG).toString(), "--create", "--topic", TopicsConfig.OUTPUT_TOPIC_NAME,
                                "--replication-factor", "1", "--partitions", String.valueOf(partitions.size())}));
            } catch (TopicExistsException e) {
                try {
                    TopicCommand.alterTopic(zkUtils,
                            new TopicCommand.TopicCommandOptions(new String[]{
                                    "--zookeeper", properties.get(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG).toString(), "--alter", "--topic", TopicsConfig.OUTPUT_TOPIC_NAME,
                                    "--partitions", String.valueOf(partitions.size())}));
                } catch (InvalidPartitionsException ex) {
                    // Do nothing
                }
            }
            try {
                TopicCommand.createTopic(zkUtils,
                        new TopicCommand.TopicCommandOptions(new String[]{
                                "--zookeeper", properties.get(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG).toString(), "--create", "--topic", TopicsConfig.SUBSCRIPTIONS_INPUT_TOPIC_NAME,
                                "--replication-factor", "1", "--partitions", "1"}));
            } catch (TopicExistsException e) {
                // Do nothing
            }
            try {
                TopicCommand.createTopic(zkUtils,
                        new TopicCommand.TopicCommandOptions(new String[]{
                                "--zookeeper", properties.get(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG).toString(), "--create", "--topic", TopicsConfig.POSTS_TOPIC_NAME,
                                "--replication-factor", "1", "--partitions", "1"}));
            } catch (TopicExistsException e) {
                // Do nothing
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            generate();
            ready = true;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    private void generate() throws Throwable {
        for (Map.Entry<String, SubscriberValue> subscriber : subscriberConfigurationService.getSubscribers().entrySet()) {
            producer.send(new ProducerRecord<>(TopicsConfig.SUBSCRIBERS_INPUT_TOPIC_NAME, Bytes.wrap(subscriber.getKey().getBytes()), subscriber.getValue()));
        }
    }

    public boolean isReady() {
        return ready;
    }
}
