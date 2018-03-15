package edu.sjsu.cs298;

import java.util.ArrayList;
import java.util.List;

public class SentenceSentimentApp {

    public static void main(String[] args) {

        List<Thread> threadList = new ArrayList<Thread>();
        int NUM_THREADS = 8;
        int limit = 1250;
        int skip;

        //

        // After 170,000 - 180,000
        for (int i = 0; i < NUM_THREADS; i++) {
            skip = 170000 + (i * limit);
            System.out.println("Thread-"+ i + " range: " + skip + "-" + (skip+limit));
            SentenceSentimentWorker worker = new SentenceSentimentWorker(limit, skip);
            Thread thread = new Thread(worker, "Thread-" + i);
            threadList.add(thread);
        }


        for (Thread t : threadList) {
            t.start();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
