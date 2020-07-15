package com.course.kafka.broker.message;

public class PromotionMessage {

    private String promotionalCode;

    public PromotionMessage(String promotionalCode) {
        this.promotionalCode = promotionalCode;
    }

    public String getPromotionalCode() {
        return promotionalCode;
    }

    public void setPromotionalCode(String promotionalCode) {
        this.promotionalCode = promotionalCode;
    }
}
