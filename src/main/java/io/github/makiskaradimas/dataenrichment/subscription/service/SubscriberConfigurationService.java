package io.github.makiskaradimas.dataenrichment.subscription.service;

import io.github.makiskaradimas.dataenrichment.subscription.SubscriberValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;

/**
 * @author Makis Karadimas
 */
@Component
@PropertySource("classpath:subscriber.properties")
public class SubscriberConfigurationService {

    private final Environment env;

    @Value("${subscriber.list}")
    private String subscribersList;

    private HashMap<String, SubscriberValue> subscribers;

    @Autowired
    public SubscriberConfigurationService(Environment env) {
        this.env = env;
    }

    @PostConstruct
    private void generateConfig() {
        subscribers = new HashMap<>();

        String[] subscriberArray = subscribersList.split(",");
        for (String subscriber : subscriberArray) {
            SubscriberValue subscriberValue = new SubscriberValue();
            subscriberValue.setHost(env.getProperty(subscriber.trim() + ".host"));
            subscriberValue.setPort(Integer.parseInt(env.getProperty(subscriber.trim() + ".port")));
            subscriberValue.setPath(env.getProperty(subscriber.trim() + ".path"));
            subscriberValue.setPartition(Integer.parseInt(env.getProperty(subscriber.trim() + ".partition")));
            String tppId = env.getProperty(subscriber.trim() + ".id");
            subscribers.put(tppId, subscriberValue);
        }
    }

    public HashMap<String, SubscriberValue> getSubscribers() {
        return subscribers;
    }
}
