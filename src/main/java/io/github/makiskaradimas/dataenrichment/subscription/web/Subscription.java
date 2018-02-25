package io.github.makiskaradimas.dataenrichment.subscription.web;

/**
 * @author Makis Karadimas
 */
public class Subscription {
    private String subscriberId;
    private String category;

    public String getSubscriberId() {
        return subscriberId;
    }

    public void setSubscriberId(String subscriberId) {
        this.subscriberId = subscriberId;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }
}
