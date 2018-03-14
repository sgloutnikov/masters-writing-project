package edu.sjsu.cs298;

import com.google.gson.*;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import edu.sjsu.cs298.pojo.ReviewSentenceSentiment;
import edu.sjsu.cs298.pojo.SentimentVector;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.mongodb.client.model.Sorts.ascending;

public class SentenceSentimentWorker implements Runnable {

    int limit;
    int skip;

    MongoClient mongoClient = new MongoClient( "24.4.4.125" , 27017 );
    MongoDatabase database = mongoClient.getDatabase("yelp_reviews");
    MongoCollection<Document> sentimentResults = database.getCollection("sentiment_results");
    MongoCollection<Document> sentimentVectors = database.getCollection("sentiment_vectors");
    MongoCollection<Document> dataCollection = database.getCollection("review_50");

    public SentenceSentimentWorker(int limit, int skip) {
        this.limit = limit;
        this.skip = skip;
    }

    public void run() {
        // Set annotators
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        System.out.println(Thread.currentThread().getName() + " starting...");
        try {
            Gson gson = new GsonBuilder().create();
            JsonParser jsonParser = new JsonParser();

            //Get input from MongoDB
            List<Document> inputList = dataCollection.find().sort(ascending("_id"))
                    .limit(limit).skip(skip).into(new ArrayList<Document>());

            List<Document> sentencesDocList = new ArrayList<Document>();

            for (Document review : inputList) {
                JsonElement reviewJson = jsonParser.parse(review.toJson());
                JsonObject reviewObject = reviewJson.getAsJsonObject();

                String reviewId = reviewObject.get("review_id").getAsString();
                String userId = reviewObject.get("user_id").getAsString();
                String sentimentVector = "";

                Annotation annotation = pipeline.process(reviewObject.get("text").getAsString());
                List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
                for (int i = 0; i < sentences.size(); i++) {
                    CoreMap sentence = sentences.get(i);
                    Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                    int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                    sentimentVector += sentiment;

                    ReviewSentenceSentiment rss = new ReviewSentenceSentiment(reviewId, userId,
                            sentence.toString(), i, sentiment);

                    Document sentenceDoc = Document.parse(gson.toJson(rss));
                    sentencesDocList.add(sentenceDoc);

                }

                SentimentVector sv = new SentimentVector(reviewId, userId, sentimentVector);
                Document sentimentVectorDoc = Document.parse(gson.toJson(sv));

                // Save to Mongo
                sentimentVectors.insertOne(sentimentVectorDoc);
                sentimentResults.insertMany(sentencesDocList);
                sentencesDocList.clear();

            }

            System.out.println(Thread.currentThread().getName() + " DONE!");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        finally {
            mongoClient.close();
        }
    }
}
