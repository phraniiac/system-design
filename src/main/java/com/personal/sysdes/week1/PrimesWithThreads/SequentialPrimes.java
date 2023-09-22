package com.personal.sysdes.week1.PrimesWithThreads;

public class SequentialPrimes {
    public int result = 1;
    boolean isPrime(int num) {
        for (int i = 3; i <= Math.sqrt(num); i++) {
            if ((num % i) == 0) {
                return false;
            }
        }
        return true;
    }
    
    public void countPrimes(int mxVal) {
        for (int i = 3; i < mxVal; i = i + 2) {
            if (isPrime(i)) {
                result += 1;
            }
        }
    }

    public static void main(String[] args) {
        int mxVAL = 1000000000;
        long startTime = System.currentTimeMillis();
        SequentialPrimes sp = new SequentialPrimes();
        sp.countPrimes(mxVAL);
        long currentTime = System.currentTimeMillis();
        System.out.println("Total Primes: " + sp.result);
        System.out.println("Total time taken: " + ((currentTime - startTime) / 1000));
    }
}
