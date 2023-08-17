package com.personal.sysdes.utils;

public class WaitGroup {

    private int counter;

    public WaitGroup() {
        this.counter = 0;
    }

    synchronized public void add() {
        counter += 1;
    }
    
    synchronized public void add(int n) throws Exception {
        counter += n;

        if (counter < 0) {
            throw new Exception("total threads to wait cannot be less than 0", new IllegalArgumentException());
        }

        if(counter == 0) {
            notifyAll();
        }
    }

    public void done() throws Exception {
        add(-1);
    }

    synchronized public void await() throws InterruptedException {
        while (counter > 0) {
            wait();
        }
    }

}
