package com.personal.sysdes.week1.connectionpool;

import com.personal.sysdes.week1.blockingqueue.BlockingQueue;
import com.personal.sysdes.utils.Utils;
import com.personal.sysdes.utils.WaitGroup;

public class ConnectionPool {
    
    // Blocking Queue.
    private final BlockingQueue connectionPool;
    // private final List<Object> queries;

    public ConnectionPool(int size) throws Exception {
        // this.queries = new ArrayList<>();

        connectionPool = new BlockingQueue(size);
        WaitGroup wg = new WaitGroup();
        for (int i = 0; i < size; i++) {
            wg.add();
            new Thread(new Runnable() {
                public void run() {
                    try {
                        Object connObj = createConnectionObj("null", "null", "null");
                        addConnection(connObj);
                        wg.done();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        wg.await();
        System.out.println("Connections created!!!!");
    }

    private Object createConnectionObj(String ip, String username, String passwd) throws Exception {
        // mimic connection open.
        Thread.sleep(Utils.getRandomInt(1, 5) * 1000);
        return new Object();
    }

    private Object getConnection() throws Exception {
        return this.connectionPool.take();
    }

    private void addConnection(Object object) throws Exception {
        this.connectionPool.put(object);
    }

    // some dummy query
    public Object performQuery(String query) throws Exception {

        long waitTimeForConnStart = 0;
        long waitTimeForConnEnd = 0;
        // Blocking call, as it will wait.
        waitTimeForConnStart = System.currentTimeMillis();
        Object connObj = getConnection();
        waitTimeForConnEnd = System.currentTimeMillis();
        System.out.println("Handling query :- " + query + "; wait_time: " + (waitTimeForConnEnd - waitTimeForConnStart));
        // mimic query execution
        Object queryRes = new Object();
        Thread.sleep(Utils.getRandomInt(1, 5) * 1000, 0);

        addConnection(connObj);
        return queryRes;
    }


    public void addNewConnection(Object connObj) throws Exception {
        addConnection(connObj);
    }

    public static void main(String[] args) throws Exception {

        // Create connection pool to hold 5 connections.
        ConnectionPool cp = new ConnectionPool(5);

        // Should be rejected
        cp.addNewConnection(new Object());

        WaitGroup wg = new WaitGroup();
        
        for (int i = 0; i < 500; i++) {
            final int j = i;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        cp.performQuery("select * from a limit 10; -- query number: " + j);
                        wg.done();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }).start();
            wg.add(1);
        }
        wg.await();

        // Sample output sequential -
        // Handling query :- select * from a limit 10; -- query number: 38; wait_time: 110130
        // Handling query :- select * from a limit 10; -- query number: 23; wait_time: 111109
        // Handling query :- select * from a limit 10; -- query number: 440; wait_time: 111075

        // query 38 is produced later, than 23rd, hence the wait time is slightly less than 23rd's.
        // There is no fairness present in a thread making a query before than another.
    }

}
