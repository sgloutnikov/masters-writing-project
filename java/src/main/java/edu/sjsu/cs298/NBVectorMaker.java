package edu.sjsu.cs298;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Sorts.ascending;

public class NBVectorMaker {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 27017;
        TupleGenerator tg = new TupleGenerator();

        MongoClient mongoClient = new MongoClient(host, port);
        MongoDatabase database = mongoClient.getDatabase("yelp_reviews");
        MongoCollection<Document> sentimentResults = database.getCollection("nb_sentiment_results");
        MongoCollection<Document> sentimentVectors = database.getCollection("nb_sentimentVectors");
        MongoCollection<Document> sentimentTuples = database.getCollection("nb_sentimentTuples");

        MongoCursor<Document> cursor = sentimentResults.find()
                .sort(ascending("review_id", "position")).iterator();
        String sentimentVector = "Init";
        String currReviewId = "Init";
        String currUserId = "Init";
        try {
            while (cursor.hasNext()) {
                Document review = cursor.next();
                String userId = review.getString("user_id");
                String reviewId = review.getString("review_id");
                String sentiment = String.valueOf(review.getInteger("sentiment"));
                // Still the same review
                if (reviewId.equals(currReviewId)) {
                    sentimentVector += sentiment;
                } else {
                    // New Review - Save sentiment vector
                    Document vectorDoc = new Document().append("review_id", currReviewId)
                            .append("user_id", currUserId)
                            .append("sentimentVector", sentimentVector);
                    sentimentVectors.insertOne(vectorDoc);
                    // check if length < 4 before generating, if so just insert sentiment vector as tuple
                    if (sentimentVector.length() < 4) {
                        Document tupleDoc = new Document().append("user_id", currUserId)
                                .append("review_id", currReviewId)
                                .append("sentimentTuple", sentimentVector);
                        sentimentTuples.insertOne(tupleDoc);
                    } else {
                        List<Document> tupleDocList = new ArrayList<Document>();
                        for (String tuple : tg.generateTuple(sentimentVector)) {
                            Document doc = new Document().append("user_id", currUserId)
                                    .append("review_id", currReviewId)
                                    .append("sentimentTuple", tuple);
                            tupleDocList.add(doc);
                        }
                        sentimentTuples.insertMany(tupleDocList);
                    }
                    // Reset
                    currReviewId = reviewId;
                    currUserId = userId;
                    sentimentVector = "";
                    sentimentVector += sentiment;
                }
            }
        } finally {
            cursor.close();
            mongoClient.close();
        }
    }
}
