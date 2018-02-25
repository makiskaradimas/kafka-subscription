package io.github.makiskaradimas.dataenrichment.subscription.web;

import io.github.makiskaradimas.dataenrichment.subscription.SubscriptionStatus;
import io.github.makiskaradimas.dataenrichment.subscription.service.SubscriberProducerService;
import io.github.makiskaradimas.dataenrichment.subscription.service.SubscriptionProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

/**
 * @author Makis Karadimas
 */
@Controller
public class SubscriptionController {

    private final SubscriberProducerService subscriberProducerService;
    private final SubscriptionProducerService subscriptionProducerService;

    @Autowired
    public SubscriptionController(SubscriberProducerService subscriberProducerService, SubscriptionProducerService subscriptionProducerService) {
        this.subscriberProducerService = subscriberProducerService;
        this.subscriptionProducerService = subscriptionProducerService;
    }

    @GetMapping(value = "/up")
    public ResponseEntity checkReadiness() {
        if (subscriberProducerService.isReady()) {
            return new ResponseEntity(HttpStatus.OK);
        } else {
            return new ResponseEntity(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    @PostMapping(value = "/subscriptions")
    public ResponseEntity createSubscription(@RequestBody Subscription subscription) {

        try {
            subscriptionProducerService.generate(subscription, SubscriptionStatus.SUBSCRIBED);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(subscription, HttpStatus.OK);
    }

    @PostMapping(value = "/unsubscriptions")
    public ResponseEntity createUnsubscription(@RequestBody Subscription subscription) {

        try {
            subscriptionProducerService.generate(subscription, SubscriptionStatus.UNSUBSCRIBED);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(subscription, HttpStatus.OK);
    }
}
