package com.course.kafka.broker.stream.feedback;

public class FeedbackMessage {

    private String feedback;

    private String Location;

    public FeedbackMessage(String feedback, String location) {
        this.feedback = feedback;
        Location = location;
    }

    public String getFeedback() {
        return feedback;
    }

    public void setFeedback(String feedback) {
        this.feedback = feedback;
    }

    public String getLocation() {
        return Location;
    }

    public void setLocation(String location) {
        Location = location;
    }
}
