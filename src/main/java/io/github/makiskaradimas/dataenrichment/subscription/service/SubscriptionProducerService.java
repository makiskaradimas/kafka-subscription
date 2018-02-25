package io.github.makiskaradimas.dataenrichment.subscription.service;

import com.google.common.io.Resources;
import io.github.makiskaradimas.dataenrichment.config.TopicsConfig;
import io.github.makiskaradimas.dataenrichment.subscription.SubscriptionStatus;
import io.github.makiskaradimas.dataenrichment.subscription.SubscriptionValue;
import io.github.makiskaradimas.dataenrichment.subscription.web.Subscription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

/**
 * @author Makis Karadimas
 */
@Component
public class SubscriptionProducerService {

    private KafkaProducer<Bytes, SubscriptionValue> producer;

    @PostConstruct
    private void init() {
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void generate(Subscription subscription, SubscriptionStatus status) {

        final SubscriptionValue subscriptionValue = new SubscriptionValue();

        subscriptionValue.setSubscriptionId(UUID.randomUUID().toString());
        subscriptionValue.setCategory(subscription.getCategory());
        subscriptionValue.setSubscriberId(subscription.getSubscriberId());
        subscriptionValue.setStatus(status);

        producer.send(new ProducerRecord<>(TopicsConfig.SUBSCRIPTIONS_INPUT_TOPIC_NAME, Bytes.wrap(subscription.getCategory().getBytes()), subscriptionValue));
    }
}
