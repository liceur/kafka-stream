package com.course.kafka.broker.message;

import java.time.LocalDateTime;

public class InventoryMessage {

    private String location;
    private String item;
    private long quantity;
    private String type;
    private LocalDateTime transactiontimel;

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public long getQuantity() {
        return quantity;
    }

    public void setQuantity(long quantity) {
        this.quantity = quantity;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public LocalDateTime getTransactiontimel() {
        return transactiontimel;
    }

    public void setTransactiontimel(LocalDateTime transactiontimel) {
        this.transactiontimel = transactiontimel;
    }

    @Override
    public String toString() {
        return "InventoryMessage{" +
                "location='" + location + '\'' +
                ", item='" + item + '\'' +
                ", quantity=" + quantity +
                ", type='" + type + '\'' +
                ", transactiontimel=" + transactiontimel +
                '}';
    }
}
