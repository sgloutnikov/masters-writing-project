package edu.sjsu.cs298;

public class SentenceSentimentApp {

    public static void main(String[] args) {

        SentenceSentimentWorker w1 = new SentenceSentimentWorker("C:\\Users\\Admin\\Downloads\\YelpDataset11\\dataset\\rev_smallaa");
        Thread t1 = new Thread(w1, "Thread-1");
        SentenceSentimentWorker w2 = new SentenceSentimentWorker("C:\\Users\\Admin\\Downloads\\YelpDataset11\\dataset\\rev_smallaa");
        Thread t2 = new Thread(w2, "Thread-2");
        SentenceSentimentWorker w3 = new SentenceSentimentWorker("C:\\Users\\Admin\\Downloads\\YelpDataset11\\dataset\\rev_smallaa");
        Thread t3 = new Thread(w3, "Thread-3");

        SentenceSentimentWorker w4 = new SentenceSentimentWorker("C:\\Users\\Admin\\Downloads\\YelpDataset11\\dataset\\rev_smallad");
        Thread t4 = new Thread(w4, "Thread-4");
        SentenceSentimentWorker w5 = new SentenceSentimentWorker("C:\\Users\\Admin\\Downloads\\YelpDataset11\\dataset\\rev_smallae");
        Thread t5 = new Thread(w5, "Thread-5");
        SentenceSentimentWorker w6 = new SentenceSentimentWorker("C:\\Users\\Admin\\Downloads\\YelpDataset11\\dataset\\rev_smallaf");
        Thread t6 = new Thread(w6, "Thread-6");

        SentenceSentimentWorker w7 = new SentenceSentimentWorker("C:\\Users\\Admin\\Downloads\\YelpDataset11\\dataset\\rev_smallag");
        Thread t7 = new Thread(w7, "Thread-7");

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
        t6.start();
        t7.start();
    }
}
