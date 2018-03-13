package edu.sjsu.cs298.pojo;

public class Review {
    private String review_id;
    private String user_id;
    private String business_id;
    private int stars;
    private String date;
    private String text;
    private int useful;
    private int funny;
    private int cool;

    public String getReview_id() {
        return review_id;
    }

    public String getUser_id() {
        return user_id;
    }

    public String getText() {
        return text;
    }
}
