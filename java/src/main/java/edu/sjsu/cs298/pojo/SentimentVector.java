package edu.sjsu.cs298.pojo;

public class SentimentVector {
    private String review_id;
    private String user_id;
    private String sentimentVector;

    public SentimentVector(String review_id, String user_id, String sentimentVector) {
        this.review_id = review_id;
        this.user_id = user_id;
        this.sentimentVector = sentimentVector;
    }
}
