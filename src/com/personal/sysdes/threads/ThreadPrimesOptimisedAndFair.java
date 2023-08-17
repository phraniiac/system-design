package com.personal.sysdes.threads;

import java.util.concurrent.atomic.AtomicInteger;

import com.personal.sysdes.utils.WaitGroup;

public class ThreadPrimesOptimisedAndFair {
    public AtomicInteger result = new AtomicInteger(1);
    public AtomicInteger currentNum = new AtomicInteger(2);

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
        public ThreadPrimesOptimisedAndFair tp1;
        public WaitGroup wg;
        public int mxVAL;
        public int threadNum;

        public PRun(ThreadPrimesOptimisedAndFair tp1, WaitGroup wg, int mxVAL, int threadNum) {
            this.tp1 = tp1;
            this.wg = wg;
            this.mxVAL = mxVAL;
            this.threadNum = threadNum;
        }

        public void run() {
            System.out.printf("Started thread %s\n", this.threadNum);
            long startTime = System.currentTimeMillis();
            long waitTimeMillis = 0;
            long waitTimeMillisStart = 0;
            long waitTimeMillisEnd = 0;

            int processedNums = 0;

            int cnum = this.tp1.currentNum.get();

            while (cnum <= this.mxVAL) {
                waitTimeMillisStart = System.currentTimeMillis();
                cnum = this.tp1.currentNum.incrementAndGet();
                waitTimeMillisEnd = System.currentTimeMillis();
                waitTimeMillis += (waitTimeMillisEnd - waitTimeMillisStart);
                if(cnum > mxVAL) {
                    break;
                }
                processedNums += 1;
                if (this.tp1.isPrime(cnum)) {
                    waitTimeMillisStart = System.currentTimeMillis();
                    tp1.result.incrementAndGet();
                    waitTimeMillisEnd = System.currentTimeMillis();
                    waitTimeMillis += (waitTimeMillisEnd - waitTimeMillisStart);
                }
                
            }
                    
            long currentTime = System.currentTimeMillis();
            System.out.printf("Total time taken by thread %s for nums %s: %s seconds with total wait time: %s \n", 
                    this.threadNum, processedNums, ((currentTime - startTime) / 1000), waitTimeMillis);
            // System.out.printf("Completed checking primes for %s to %s \n ", this.startVal, this.mxVal);
            try {
                wg.done();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int mxVAL = 1000000000;
        int numThreads = 1000;
        
        ThreadPrimesOptimisedAndFair tp1 = new ThreadPrimesOptimisedAndFair();

        WaitGroup wg = new WaitGroup();

        for (int i = 0; i < numThreads; i++) {
            wg.add(1);
            Thread t1 = new Thread(new PRun(tp1, wg, mxVAL, i));
            t1.start();
        }

        wg.await();
        System.out.println("Total Primes: " + tp1.result);
    }
}
