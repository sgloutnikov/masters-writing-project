package edu.sjsu.cs298;

import java.util.ArrayList;
import java.util.List;

public class SentenceSentimentApp {

    public static void main(String[] args) {

        //SentenceSentimentWorker w1 = new SentenceSentimentWorker(50, 0);
        //Thread t1 = new Thread(w1, "Thread-1");
        //t1.start();

        List<Thread> threadList = new ArrayList<Thread>();

        int limit = 1000;
        int skip;


        // Done 25,000

        // 26,000 - 36,000
        for (int i = 1; i <= 10; i++) {
            skip = 25000 + (i * limit);
            System.out.println(i + " skip " + skip);
            SentenceSentimentWorker worker = new SentenceSentimentWorker(limit, skip);
            Thread thread = new Thread(worker, "Thread-" + i);
            threadList.add(thread);
        }

        for (Thread t : threadList) {
            t.start();
        }
    }
}
