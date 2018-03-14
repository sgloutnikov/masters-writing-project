package edu.sjsu.cs298;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import edu.sjsu.cs298.pojo.Review;
import edu.sjsu.cs298.pojo.ReviewSentenceSentiment;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SentimentTest {

    public static void main(String[] args) {
        /*
        String line = "";

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation annotation = pipeline.process(line);
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            System.out.println(sentence.toString());
            System.out.println(sentiment);
        }
        */

        String fileInput = "C:\\Users\\Admin\\Downloads\\YelpDataset11\\dataset\\review_small_split.jsonaa";

        MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
        MongoDatabase database = mongoClient.getDatabase("yelp_reviews");
        MongoCollection<Document> collection = database.getCollection("sentiment_results");

        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        System.out.println("Starting...");
        /*
        try {
            FileInputStream fileInputStream = new FileInputStream(fileInput);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            JsonReader jsonReader = new JsonReader(inputStreamReader);
            jsonReader.setLenient(true);

            Gson gson = new GsonBuilder().create();
            int count = 0;

            List<Document> docList = new ArrayList<Document>();
            while (jsonReader.hasNext()){
                Review r = gson.fromJson(jsonReader, Review.class);

                Annotation annotation = pipeline.process(r.getText());
                List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
                for (int i = 0; i < sentences.size(); i++) {
                    CoreMap sentence = sentences.get(i);
                    Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                    int sentiment = RNNCoreAnnotations.getPredictedClass(tree);

                    ReviewSentenceSentiment rss = new ReviewSentenceSentiment(r.getReview_id(), sentence.toString(),
                            i, sentiment);

                    Document doc = Document.parse(gson.toJson(rss));
                    docList.add(doc);
                    System.out.println(count);
                    count++;
                }

                if (jsonReader.peek() == JsonToken.END_DOCUMENT) {
                    System.out.println("Finished document. Exiting.");
                    break;
                }
            }

            collection.insertMany(docList);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        */

    }
}