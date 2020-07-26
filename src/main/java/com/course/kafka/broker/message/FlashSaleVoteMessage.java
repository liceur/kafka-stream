package com.course.kafka.broker.message;

public class FlashSaleVoteMessage {

    private String customerId;
    private String itemName;

    public FlashSaleVoteMessage(String customerId, String itemName) {
        this.customerId = customerId;
        this.itemName = itemName;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }
}
