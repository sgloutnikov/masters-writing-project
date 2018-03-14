package edu.sjsu.cs298;

import java.util.ArrayList;
import java.util.List;

public class SentenceSentimentApp {

    public static void main(String[] args) {

        List<Thread> threadList = new ArrayList<Thread>();
        int NUM_THREADS = 8;
        int limit = 1250;
        int skip = 130000;

        //

        // After 130,000 - 140,000
        for (int i = 0; i < NUM_THREADS; i++) {
            skip = skip + (i * limit);
            System.out.println("Thread-"+ i + " range: " + skip + "-" + (skip+limit));
            SentenceSentimentWorker worker = new SentenceSentimentWorker(limit, skip);
            Thread thread = new Thread(worker, "Thread-" + i);
            threadList.add(thread);
        }

        for (Thread t : threadList) {
            t.start();
        }
    }
}
