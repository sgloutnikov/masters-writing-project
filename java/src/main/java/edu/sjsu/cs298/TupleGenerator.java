package edu.sjsu.cs298;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Sorts.ascending;

public class TupleGenerator {

    public static void main(String[] args) {
        String host = "";
        int port = 27017;
        int skip = 0;
        TupleGenerator tg = new TupleGenerator();

        MongoClient mongoClient = new MongoClient(host, port);
        MongoDatabase database = mongoClient.getDatabase("yelp_reviews");
        MongoCollection<Document> sentimentVectors = database.getCollection("sentimentVectors");
        MongoCollection<Document> sentimentTuples = database.getCollection("sentimentTuples");

        MongoCursor<Document> cursor = sentimentVectors.find().sort(ascending("_id"))
                .skip(skip).iterator();

        try {
            while (cursor.hasNext()) {
                Document sentimentVectorDoc = cursor.next();
                String userId = sentimentVectorDoc.getString("user_id");
                String reviewId = sentimentVectorDoc.getString("review_id");
                String sentimentVector = sentimentVectorDoc.getString("sentimentVector");

                // check if length < 4 before generating, if so just insert sentiment vector as tuple
                if (sentimentVector.length() < 4) {
                    Document tupleDoc = new Document().append("user_id", userId)
                            .append("review_id", reviewId)
                            .append("sentimentTuple", sentimentVector);
                    sentimentTuples.insertOne(tupleDoc);
                } else {
                    List<Document> tupleDocList = new ArrayList<Document>();
                    for (String tuple : tg.generateTuple(sentimentVector)) {
                        Document doc = new Document().append("user_id", userId)
                                .append("review_id", reviewId)
                                .append("sentimentTuple", tuple);
                        tupleDocList.add(doc);
                    }
                    sentimentTuples.insertMany(tupleDocList);
                }
            }
        } finally {
            cursor.close();
            mongoClient.close();
        }

    }

    /*
    Generate all tuples of length 3 or larger.
     */
    public List<String> generateTuple(String sVector) {
        List<String> sentimentTuples = new ArrayList<String>();
        int length = sVector.length();
        int k = length - 1;

        while (k > 2) {
            for (int i = 0; i + k <= length; i++) {
                String tuple = sVector.substring(i, i+k);
                sentimentTuples.add(tuple);
            }
            k--;
        }

        return sentimentTuples;
    }
}
