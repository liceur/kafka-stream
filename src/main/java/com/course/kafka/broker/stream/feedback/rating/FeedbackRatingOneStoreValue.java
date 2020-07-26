package com.course.kafka.broker.stream.feedback.rating;

public class FeedbackRatingOneStoreValue {

    private long countRating;
    private long sumRating;


    public long getCountRating() {
        return countRating;
    }

    public void setCountRating(long countRating) {
        this.countRating = countRating;
    }

    public long getSumRating() {
        return sumRating;
    }

    public void setSumRating(long sumRating) {
        this.sumRating = sumRating;
    }

    @Override
    public String toString() {
        return "FeedbackRatingOneStoreValue{" +
                "countRating=" + countRating +
                ", sumRating=" + sumRating +
                '}';
    }
}
