package com.personal.sysdes.week1.connectionpool;

import com.personal.sysdes.week1.blockingqueue.BlockingQueue;
import com.personal.sysdes.utils.Utils;
import com.personal.sysdes.utils.WaitGroup;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Thread.sleep;

public class ConnectionPoolFair {

    public static class ResultSet {
        private boolean isResultPrepared;
        private final String query;
        private Object results;

        public ResultSet(String query) {
            this.isResultPrepared = false;
            this.results = null;
            this.query = query;
        }

        public void setResult(boolean status, Object results) {
            this.isResultPrepared = true;
            this.results = results;
        }

        public boolean getStatus() {
            return isResultPrepared;
        }

        public Object getResult() {
            return results;
        }

        public String getQuery() {
            return query;
        }
    }
    
    // Blocking Queue.
    private final BlockingQueue connectionPool;
    private final List<ResultSet> queriesQ;

    public ConnectionPoolFair(int size) throws Exception {
        this.queriesQ = new ArrayList<>();

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
        sleep(Utils.getRandomInt(1, 5) * 1000L);
        return new Object();
    }

    private Object getConnection() throws Exception {
        return this.connectionPool.take();
    }

    private void addConnection(Object object) throws Exception {
        this.connectionPool.put(object);
    }

    synchronized private void addQuery(ResultSet query) {
        this.queriesQ.add(query);
    }

    // caller should check if it's not empty.
    synchronized private ResultSet getQuery() {
        return this.queriesQ.remove(0);
    }

    // pops the first element from queries and executes it.
    // start event loop on a new thread.
    public void startEventLoop() throws InterruptedException, Exception {
        System.out.println("Event Loop Started!!!!!!!");
        while (true) {
            if (!this.queriesQ.isEmpty()) {
                long waitTimeForConnStart = System.currentTimeMillis();
                ResultSet rs = getQuery();
                Object connObj = getConnection();
                long waitTimeForConnEnd = System.currentTimeMillis();
                System.out.println("Handling query :- " + rs.getQuery() + "; wait_time: " + (waitTimeForConnEnd - waitTimeForConnStart));
                new Thread(new Runnable() {
                    public void run() {
                        try {
                            performQuery(rs.getQuery());
                            addConnection(connObj);
                        } catch (Exception e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }).start();
            }
            else {
                sleep(60);
            }
        }
    }

    public ResultSet performQueryFair(String query) throws InterruptedException {
        ResultSet rs = new ResultSet(query);
        // Java passes the reference by value. In short, the values will be reflected in the thread
        // so we can block on the status property.
        this.addQuery(rs);
        
        while(!rs.getStatus()) {
            sleep(200);
        }
        return rs;
    }

    // some dummy query
    private Object performQuery(String query) throws Exception {
        // mimic query execution
        Object queryRes = new Object();
        sleep(Utils.getRandomInt(1, 5) * 1000L, 0);
        return queryRes;
    }


    public void addNewConnection(Object connObj) throws Exception {
        addConnection(connObj);
    }

    public static void main(String[] args) throws Exception {

        // Create connection pool to hold 5 connections.
        ConnectionPoolFair cp = new ConnectionPoolFair(5);

        // Start the event loop in background.
        new Thread(new Runnable() {
            public void run() {
                try {
                    cp.startEventLoop();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }).start();

        WaitGroup wg = new WaitGroup();
        
        for (int i = 0; i < 500; i++) {
            final int j = i;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        ResultSet rs = cp.performQueryFair("select * from a limit 10; -- query number: " + j);
                        wg.done();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }).start();
            wg.add(1);
            sleep(20);
        }
        wg.await();
    }

}
