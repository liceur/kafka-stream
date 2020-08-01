package com.course.kafka.broker.message;

import java.time.LocalDateTime;

public class WebColorVoteMessage {

    private LocalDateTime voteDateTime;
    private String color;

    public LocalDateTime getVoteDateTime() {
        return voteDateTime;
    }

    public void setVoteDateTime(LocalDateTime voteDateTime) {
        this.voteDateTime = voteDateTime;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }
}
