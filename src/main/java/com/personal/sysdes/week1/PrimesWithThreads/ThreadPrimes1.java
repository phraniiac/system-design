package com.personal.sysdes.week1.PrimesWithThreads;

import com.personal.sysdes.utils.WaitGroup;

import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPrimes1 {
    public AtomicInteger result = new AtomicInteger(1);

    boolean isPrime(int num) {
        if((num & 1) == 0) {
            return false;
        }
        for (int i = 3; i <= Math.sqrt(num); i += 2) {
            if ((num % i) == 0) {
                return false;
            }
        }
        return true;
    }
    
    public void countPrimes(int startVal, int mxVal) {
        System.out.printf("Checking for batch: %s to %s%n", startVal, mxVal);
        for (int i = startVal; i < mxVal; i++) {
            if (isPrime(i)) {
                result.addAndGet(1);
            }
        }
    }

    public static class PRun implements Runnable {
        public int startVal;
        public int mxVal;
        public ThreadPrimes1 tp1;

        public WaitGroup wg;
        public PRun(int startVal, int mxVal, ThreadPrimes1 tp1, WaitGroup wg) {
            this.startVal = startVal;
            this.mxVal = mxVal;
            this.tp1 = tp1;
            this.wg = wg;
        }

        public void run() {
            long startTime = System.currentTimeMillis();
            this.tp1.countPrimes(this.startVal, this.mxVal);
            // System.out.printf("Completed checking primes for %s to %s \n ", this.startVal, this.mxVal);
            try {
                wg.done();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
                    
            long currentTime = System.currentTimeMillis();
            System.out.printf("Total time taken for batch %s to %s: %s seconds \n", this.startVal, this.mxVal, ((currentTime - startTime) / 1000));
        }
    }

    public static void main(String[] args) throws Exception {
        int mxVAL = 100000000;
        int numThreads = 10;
        
        ThreadPrimes1 tp1 = new ThreadPrimes1();
        int batch = mxVAL / numThreads;

        WaitGroup wg = new WaitGroup();

        for (int i = 0; i < numThreads; i++) {
            wg.add(1);
            Thread t1 = new Thread(new PRun(i * batch, ((i + 1) * batch) - 1, tp1, wg));
            t1.start();
        }

        wg.await();
        System.out.println("Total Primes: " + tp1.result);
    }
}
