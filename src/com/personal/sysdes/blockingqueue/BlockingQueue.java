package com.personal.sysdes.blockingqueue;

import java.util.ArrayList;
import java.util.List;

public class BlockingQueue {
    
    private List<Object> queue;
    private final Integer size;

    public BlockingQueue(int size) {
        this.queue = new ArrayList<>();
        this.size = size;
    }

    synchronized public void put(Object ele) throws InterruptedException {
        // Try for 5 times, if you can add the element, then interrupt the current thread.
        int tries = 5;
        while (tries > 0) {
            if (this.queue.size() < this.size) {
                this.queue.add(ele);
                notifyAll();
                return;
            }
            else {
                wait(1000);
                tries -= 1;
            }
        }
        System.out.println("Interrupted because queue is full for a long time.");
        Thread.currentThread().interrupt();
    }

    synchronized public Object take() throws InterruptedException {
        // int tries = 5;

        while (true) {
            if (queue.size() > 0) {
                Object ele = queue.remove(0);
                return ele;
            } else {
                wait(1000);
            }
        }
    }

    synchronized public int getCurrentSize() {
        return this.queue.size();
    }

}
