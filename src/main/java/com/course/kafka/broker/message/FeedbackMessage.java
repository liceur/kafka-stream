package com.course.kafka.broker.message;

public class FeedbackMessage {

    private String feedback;

    private String Location;

    private long rating;

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

    public long getRating() {
        return rating;
    }

    public void setRating(long rating) {
        this.rating = rating;
    }
}
