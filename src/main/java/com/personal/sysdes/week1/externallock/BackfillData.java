package com.personal.sysdes.week1.externallock;

import com.personal.sysdes.utils.WaitGroup;
import com.personal.sysdes.week1.blockingqueue.BlockingQueue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.sql.*;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


// Used a distributed lock.
public class BackfillData {

    Map<String, BlockingQueue> connObjects = new HashMap<>();
    String metaFile = "/tmp/backfill/__merchant__/runmeta.txt";
    String lockFile = "/tmp/backfill/__merchant__/lock.txt";
    String merchantPlaceholder = "__merchant__";

    AtomicInteger criticalOverlap = new AtomicInteger(0);

    public BackfillData() {
        try {
            initDBConnections();
        } catch (InterruptedException | ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void initDBConnections() throws InterruptedException, ClassNotFoundException, SQLException {
        // for each shard initialize 5 DB connections.
        int size = 5;
        List<String> shardList = new ArrayList<String>() {{
            add("SHARD1");
//            add("SHARD2");
//            add("SHARD3");
        }};

        Class.forName("com.amazon.redshift.jdbc.Driver");

        System.out.println("Connecting to database...");
        Properties props = new Properties();

        String MasterUsername = "analytics";
        String MasterUserPassword = "sh6OdK1fNC5u";

        String dbURL = "jdbc:redshift://test001.c6t0r1dd8hdp.us-east-1.redshift.amazonaws.com/analytics";

        //Uncomment the following line if using a keystore.
        props.setProperty("user", MasterUsername);
        props.setProperty("password", MasterUserPassword);


        for (String shard:
                shardList) {
            BlockingQueue q = new BlockingQueue(size);
            for (int i = 0; i < size; i++) {
                Connection conn = null;
                conn = DriverManager.getConnection(dbURL, props);
                if (conn != null) {
                    System.out.println("Shard: " + shard + "; Conn Done: " + i);
                    q.put(conn);
                }
            }
            this.connObjects.put(shard, q);
        }

    }

    private void closeConnections() throws SQLException {
        System.out.println("Closing connections!!");
        for (String key: this.connObjects.keySet()) {
            BlockingQueue q = this.connObjects.get(key);
            for (int i = 0; i < q.getCurrentSize(); i++) {
                Connection conn = (Connection) q.takeIndex(i);
                conn.close();
            }
        }
    }

    private void createInitFile(String merchant) throws IOException {

        File f = new File(metaFile.replace(merchantPlaceholder, merchant));
        f.getParentFile().mkdirs();
        if(f.exists()) {
            return;
        }

        List<String> finalFileContents = new ArrayList<>();
        // start date
        LocalDate dt = LocalDate.of(2022, 9, 1);
        LocalDate finalEndDate = LocalDate.of(2023, 9, 1);

        while(dt.isBefore(finalEndDate)) {
            YearMonth ym = YearMonth.of(dt.getYear(), dt.getMonth());
            int daysInMonth = ym.lengthOfMonth();
            StringBuilder binaryStrBuilder = new StringBuilder(daysInMonth);
            for (int i = 1; i <= daysInMonth; i++) {
                binaryStrBuilder.append("0");
            }

            String finalStrBuilder = dt.getMonthValue() +
                    "__" +
                    dt.getYear() +
                    "||" +
                    binaryStrBuilder;

            finalFileContents.add(finalStrBuilder);
            dt = dt.plusMonths(1);
        }

        FileWriter fw = new FileWriter(f);
        PrintWriter printWriter = new PrintWriter(fw);
        for (String ln :
                finalFileContents) {
            printWriter.println(ln);
        }
        printWriter.close();
    }

    private boolean merchantInitialised(String merchant) {
        File f = new File(metaFile.replace(merchantPlaceholder, merchant));
        return f.exists();
    }

    private String[] getProcessingRange(String merchant, int numDays) throws IOException {
        // get merchant file.

        String[] range = new String[2];
        range[0] = "-1";
        range[1] = "-1";
        List<String> lines = Files.readAllLines(Paths.get(metaFile.replace(merchantPlaceholder, merchant)));

        for (String ln : lines) {
            String[] sections = ln.split("\\|\\|");
            String month = sections[0].split("__")[0];
            String year = sections[0].split("__")[1];

            int i = 0;
            while (i < sections[1].length()) {
                if (sections[1].charAt(i) == '0') {
                    int j = i;
                    while (j < sections[1].length() &&
                            (sections[1].charAt(j) == '0') &&
                            ((j - i) + 1 < numDays)) {
                        j += 1;
                    }
                    range[0] = String.valueOf(i + 1) + "-" + month + "-" + year;
                    range[1] = String.valueOf(Math.min((j + 1),
                            YearMonth.of(Integer.parseInt(year), Integer.parseInt(month)).lengthOfMonth()))
                            + "-" + month + "-" + year;
                    break;
                }
                i++;
            }
        }
        return range;
    }

    // Acquire merchant level lock.
    private long createAndGetExternalLock(String merchant) throws InterruptedException, IOException {
        int tries = 5;
        while (tries > 0) {
            if (!lockFilePresent(merchant)) {
                long threadTtoken = System.currentTimeMillis();
                createLockFileWithContent(merchant, threadTtoken);
                return threadTtoken;
            }
            tries -= 1;
            Thread.sleep(1000);
        }
        return -1;
    }

    // We add the current timestamp as the token, so that even if multiple threads are able to create this
    // file (lock), the update can only be done by one thread by again checking the token.
    // Since the processing is idempotent, this is reasonable trade-off for execution time.
    private void createLockFileWithContent(String merchant, long token) throws IOException {
        File f = new File(lockFile.replace(merchantPlaceholder, merchant));
        f.getParentFile().mkdirs();
        FileWriter fw = new FileWriter(f);

        PrintWriter pw = new PrintWriter(fw);
        pw.println(token);
        pw.close();
    }

    private boolean lockFilePresent(String merchant) {
        File f = new File(lockFile.replace(merchantPlaceholder, merchant));
        return f.exists();
    }

    // Only the thread which has accurate token is allowed to perform update.
    // Other threads are ignored, and they re-process with newer interval.
    private void updateResultAndDeleteExternalLock(String merchant, long updateToken, String[] range) throws IOException {
        if (!lockFilePresent(merchant)) {
            return;
        }

        try {
            List<String> lockLines = Files.readAllLines(Paths.get(lockFile.replace(merchantPlaceholder, merchant)));
            String fileContent = lockLines.get(0);
            if (Objects.equals(fileContent.trim(), String.valueOf(updateToken))) {
                if (!range[0].equals("-1")) {

                    int dtStart = Integer.parseInt(range[0].split("-")[0]) - 1,
                            dtEnd = Integer.parseInt(range[1].split("-")[0]) - 1;
                    int month = Integer.parseInt(range[0].split("-")[1]);
                    int year = Integer.parseInt(range[0].split("-")[2]);

                    List<String> metaLines = Files.readAllLines(Paths.get(metaFile.replace(merchantPlaceholder, merchant)));
                    // update bookkeeping file.
                    List<String> newLines = new ArrayList<>();
                    FileWriter fw = new FileWriter(metaFile.replace(merchantPlaceholder, merchant));
                    fw.flush();
                    PrintWriter pw = new PrintWriter(fw);
                    for (String ln :
                            metaLines) {
                        String[] sections = ln.split("\\|\\|");
                        if (sections[0].split("__")[0].equals(String.valueOf(month)) &&
                                sections[0].split("__")[1].equals(String.valueOf(year))) {
                            StringBuilder sb = new StringBuilder(sections[1]);
                            for (int i = dtStart; i <= dtEnd; i++) {
                                sb.setCharAt(i, '1');
                            }
                            String finalStr = sections[0] + "||" + sb;
                            pw.println(finalStr);
                        } else {
                            pw.println(ln);
                        }
                    }
                    pw.close();
                }
                deleteLockFile(merchant);
            }
            else {
                criticalOverlap.addAndGet(1);
            }
        }
        catch (NoSuchFileException e) {
            // Can happen that the actual owner thread of the lock file deleted the lock file,
            // before this thread can execute.
            criticalOverlap.addAndGet(1);
        }
    }

    private void deleteLockFile(String merchant) {
        File f = new File(lockFile.replace(merchantPlaceholder, merchant));
        f.delete();
    }

    private void syncCode() {
        String[] args = new String[]{"/opt/anaconda3/bin/s4cmd",
                "put", "-rf",
                "/tmp/backfill/", "s3://br-user/pranav.sharma/backfill/v1"};
        try {
            ProcessBuilder procB = new ProcessBuilder(args);
//                        procB.inheritIO();
            Process proc = procB.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Thread startSyncThread() {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    syncCode();
                    System.out.println("Sync process started.");
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                // One last sync.
                System.out.println("Final sync of meta.");
                syncCode();
            }
        });
        return t;
    }

    public void processing(Connection connObj, String merchant, Date startDate, Date endDate) throws SQLException {

        System.out.println("Running Query: " + merchant + "; " + startDate + "; " + endDate);
        // get a merchant from list
        Statement stmt = connObj.createStatement();
        String sql = "select count(*) cnt from neimanmarcus.browse_sessions;";
        ResultSet rs = stmt.executeQuery(sql);

//        rs.last();
        while(rs.next()) {
            int rowCount = rs.getInt(1);
            System.out.println("ROW_COUNT: " + rowCount);
        }
        stmt.close();
    }

    public static void main(String[] args) throws InterruptedException, IOException, SQLException {
        List<String> merchantArray = new ArrayList<String>() {{
            add("neimanmarcus");
            add("globalindustrial");
            add("albertsons");
            add("totalwine");
        }};

        Set<String> initMerchants = new HashSet<>();
        Set<String> finishedMerchants = new HashSet<>();

        AtomicInteger totalIterations = new AtomicInteger(0);
        AtomicInteger merchantPointer = new AtomicInteger(0);

        BackfillData bd = new BackfillData();

        Thread syncerThread = bd.startSyncThread();
        syncerThread.join();
        syncerThread.start();

        int numThreads = 15;
        long totalIterationBreakOff = 1000;

        int processingDays = 3;

        WaitGroup wg = new WaitGroup();
        for (int i = 0; i < numThreads; i++) {
            int finalI = i;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    wg.add();
                    while (totalIterations.getAndAdd(1) < totalIterationBreakOff) {
                        if (merchantPointer.get() == merchantArray.size()) {
                            merchantPointer.set(0);
                        }
                        String merchant = merchantArray.get(merchantPointer.getAndAdd(1) % merchantArray.size());

                        System.out.println("Thread num: " + String.valueOf(finalI) + " " + merchant);

                        if (!initMerchants.contains(merchant)) {
                            if (!bd.merchantInitialised(merchant)) {
                                try {
                                    bd.createInitFile(merchant);
                                    initMerchants.add(merchant);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }

                        if (merchantArray.size() == finishedMerchants.size()) {
                            try {
                                wg.done();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            return;
                        }

                        try {
                            // to run multiple versions of this script, we maintain the bookkeeping
                            // and lock external to this application.
                            // This will help in parallelizing, this already parallelized script across
                            // multiple machines, if required.
                            long updateToken = bd.createAndGetExternalLock(merchant);
                            if (updateToken == -1) {
                                continue;
                            }
                            String[] range = bd.getProcessingRange(merchant, processingDays);
                            if (range[0].equals("-1")) {
                                finishedMerchants.add(merchant);
                            }

                            System.out.println(merchant + "--------" + range[0] + "----" + range[1]);
                            Date startDate = null, endDate = null;
                            Object connObj = bd.connObjects.get("SHARD1").take();
                            bd.processing((Connection) connObj, merchant, startDate, endDate);
                            bd.updateResultAndDeleteExternalLock(merchant, updateToken, range);
                            bd.connObjects.get("SHARD1").put(connObj);
                        } catch (InterruptedException | IOException | SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    try {
                        wg.done();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println("Finished thread: " + finalI);
                }
            });
            t.start();
        }

        wg.await();
        syncerThread.interrupt();

        bd.closeConnections();

        System.out.println("========================= CRITICAL_OVERLAP: " + bd.criticalOverlap);
    }

}
