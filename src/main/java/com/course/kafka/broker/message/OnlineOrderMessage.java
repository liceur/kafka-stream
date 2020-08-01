package com.course.kafka.broker.message;

import java.time.LocalDateTime;

public class OnlineOrderMessage {

    private String OnlineOrderNumber;
    private LocalDateTime orderDateTime;
    private int totalAmount;
    private String userName;

    public String getOnlineOrderNumber() {
        return OnlineOrderNumber;
    }

    public void setOnlineOrderNumber(String onlineOrderNumber) {
        OnlineOrderNumber = onlineOrderNumber;
    }

    public LocalDateTime getOrderDateTime() {
        return orderDateTime;
    }

    public void setOrderDateTime(LocalDateTime orderDateTime) {
        this.orderDateTime = orderDateTime;
    }

    public int getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(int totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }
}
