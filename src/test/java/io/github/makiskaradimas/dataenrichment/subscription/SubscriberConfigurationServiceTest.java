package io.github.makiskaradimas.dataenrichment.subscription;

import io.github.makiskaradimas.dataenrichment.subscription.service.SubscriptionProducerService;
import io.github.makiskaradimas.dataenrichment.subscription.service.SubscriberConfigurationService;
import io.github.makiskaradimas.dataenrichment.subscription.service.SubscriberProducerService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Makis Karadimas
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@DirtiesContext
public class SubscriberConfigurationServiceTest {

    @MockBean
    private SubscriptionProducerService subscriptionProducerService;
    @MockBean
    private SubscriberProducerService subscriberProducerService;
    @Autowired
    private SubscriberConfigurationService subscriberConfigurationService;

    @Test
    public void test() throws Exception {
        assertThat(subscriberConfigurationService.getSubscribers().get("TESTSUBSCRIBER1").getHost()).isEqualTo("subscriber-one");
        assertThat(subscriberConfigurationService.getSubscribers().get("TESTSUBSCRIBER1").getPort()).isEqualTo(3002);
        assertThat(subscriberConfigurationService.getSubscribers().get("TESTSUBSCRIBER1").getPath()).isEqualTo("/path/test");
        assertThat(subscriberConfigurationService.getSubscribers().get("TESTSUBSCRIBER1").getPartition()).isEqualTo(0);
    }
}
