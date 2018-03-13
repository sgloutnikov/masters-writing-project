package edu.sjsu.cs298.pojo;

public class ReviewSentenceSentiment {
    private String review_id;
    private String sentence;
    private int position;
    private int sentiment;

    public ReviewSentenceSentiment(String review_id, String sentence, int position, int sentiment) {
        this.review_id = review_id;
        this.sentence = sentence;
        this.position = position;
        this.sentiment = sentiment;
    }
}
