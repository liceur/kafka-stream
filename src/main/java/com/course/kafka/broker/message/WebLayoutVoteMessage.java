package com.course.kafka.broker.message;

import java.time.LocalDateTime;

public class WebLayoutVoteMessage {

    private LocalDateTime voteDateTime;
    private String layout;

    public LocalDateTime getVoteDateTime() {
        return voteDateTime;
    }

    public void setVoteDateTime(LocalDateTime voteDateTime) {
        this.voteDateTime = voteDateTime;
    }

    public String getLayout() {
        return layout;
    }

    public void setLayout(String layout) {
        this.layout = layout;
    }
}
