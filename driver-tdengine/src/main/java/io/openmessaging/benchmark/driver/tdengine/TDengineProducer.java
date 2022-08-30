/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.tdengine;

import com.taosdata.jdbc.TSDBPreparedStatement;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;







public class TDengineProducer {
    private static final Logger log = LoggerFactory.getLogger(TDengineProducer.class);

    private ArrayBlockingQueue<Object[]> queue = new ArrayBlockingQueue<>(500000);
    private Thread workThread;
    private String topic;
    private String tableName;
    private boolean closing = false;
    private Config config;
    private long startNano;
    private long startTs;
    // public ArrayList<Long> timeDelay = new ArrayList<Long>();
    public Long threadID;

    public TDengineProducer(String topic, String tableName, Config config) {
        this.topic = topic;
        this.config = config;
        this.tableName = tableName;
        this.startNano = System.nanoTime();
        this.startTs = System.currentTimeMillis() * 1000000;
        
        if (config.useStmt) {
            this.workThread = new Thread(() -> {
                try {
                    runStmt();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            threadID = workThread.getId();
            // System.out.println("threadID=====" + threadID);
        } else {
            this.workThread = new Thread(() -> {
                try {
                    run();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            threadID = workThread.getId();
            // System.out.println("threadID=====" + threadID);
        }
        workThread.start();
    }

    public boolean send(byte[] payload, CompletableFuture<Void> future) throws InterruptedException {
        long ts = System.nanoTime() - startNano + startTs;
        // [ts, payload, future]
        return queue.offer(new Object[]{ts, new String(payload), future}, 10, TimeUnit.MILLISECONDS);
    }



    public void runStmt() throws IOException{
        Connection conn = null;
        Statement stmt = null;
        ArrayList<Long> timeDelay = new ArrayList<Long>();
        try {
            String jdbcUrl = config.jdbcURL;
            conn = DriverManager.getConnection(jdbcUrl);
            stmt = conn.createStatement();
            stmt.executeUpdate("use " + config.database);
            ArrayList<Long> tsBuffer = new ArrayList<>();
            ArrayList<String> payloadBuffer = new ArrayList<>();
            String psql = "INSERT INTO ? " + "VALUES(?, ?)";
            try (TSDBPreparedStatement pst = (TSDBPreparedStatement) conn.prepareStatement(psql)) {
                log.info("setTableName: {}", tableName);
                pst.setTableName(tableName);
                long rows = 0;
                long startTime = 0;
                while (!closing) {
                    try {
                        Object[] item = queue.poll();
                        if (item != null) {
                            Long ts = (Long) item[0];
                            String payload = (String) item[1];
                            CompletableFuture<Void> future = (CompletableFuture<Void>) item[2];
                            // mark message sent successfully
                            future.complete(null);
                            tsBuffer.add(ts);
                            payloadBuffer.add(payload);
                            if (startTime == 0) {
                                startTime = System.nanoTime(); 
                            }
                            rows++;
                            if (tsBuffer.size() == config.maxBatchSize) {                                
                                flushStmt(pst, tsBuffer, payloadBuffer, tableName);
                                Long endTime = System.nanoTime();
                                timeDelay.add(endTime - startTime);
                                startTime = 0;
                            }
                        } else {
                            if (tsBuffer.size() > 0) {                                
                                flushStmt(pst, tsBuffer, payloadBuffer, tableName);
                                Long endTime = System.nanoTime();
                                timeDelay.add(endTime - startTime);
                                startTime = 0;
                            } else {
                                Thread.sleep(3);
                            }                        
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (SQLException e) {
                        log.info(e.getMessage());
                    }
                }
                if (tsBuffer.size() > 0) {
                    flushStmt(pst, tsBuffer, payloadBuffer, tableName);
                    Long endTime = System.nanoTime();
                    timeDelay.add(endTime - startTime);
                    startTime = 0;
                }
                log.info("====producer rows: " + rows);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                
                stmt.close();
                conn.close();

                
            } catch (SQLException e) {
            }
        }
        String filename = String.format("/tmp/omb/producer/%s.txt",threadID.toString());
        File file = new File(filename);
        FileWriter writer = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(writer);
        for(int i=0;i<timeDelay.size();i++){
            String line = timeDelay.get(i).toString();
            bw.write(line);
            bw.newLine();
        }
        bw.close();
        
    }

    public void flushStmt(TSDBPreparedStatement pst, ArrayList<Long> tsBuffer,
                          ArrayList<String> payloadBuffer, String tableName) throws SQLException {
        pst.setTimestamp(0, tsBuffer);
        pst.setString(1, payloadBuffer, config.varcharLen);
        pst.columnDataAddBatch();
        pst.columnDataExecuteBatch();
        pst.setTableName(tableName);
        tsBuffer.clear();
        payloadBuffer.clear();
    }

    public void run() throws IOException {
        Connection conn = null;
        Statement stmt = null;
        ArrayList<Long> timeDelay = new ArrayList<Long>();
        try {
            String jdbcUrl = config.jdbcURL;
            conn = DriverManager.getConnection(jdbcUrl);
            stmt = conn.createStatement();
            stmt.executeUpdate("use " + config.database);
            List<String> values = new ArrayList<>();
            int rows = 0;
            long startTime = 0;
            while (!closing) {
                try {
                    Object[] item = queue.poll();
                    if (item != null) {
                        Object ts = item[0];
                        Object payload = item[1];
                        CompletableFuture<Void> future = (CompletableFuture<Void>) item[2];
                        // mark message sent successfully
                        future.complete(null);
                        values.add(" (" + ts + ",'" + payload + "')");
                        if (startTime == 0) {
                            startTime = System.nanoTime();
                        }
                        rows++;
                        if (values.size() == config.maxBatchSize) {                            
                            flush(stmt, tableName, values);
                            Long endTime = System.nanoTime();
                            timeDelay.add((endTime - startTime));
                            startTime = 0;
                        }
                    } else {
                        if (values.size() > 0) {
                            flush(stmt, tableName, values);
                            Long endTime = System.nanoTime();
                            timeDelay.add(endTime - startTime);
                            startTime = 0;
                        }
                        Thread.sleep(3);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (SQLException e) {
                    log.info(e.getMessage());
                }
            }
            if (values.size() > 0) {
                flush(stmt, tableName, values);
                Long endTime = System.nanoTime();
                timeDelay.add(endTime - startTime);
                startTime = 0;
            }
            log.info("====producer rows: " + rows);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {

                stmt.close();
                conn.close();
            } catch (SQLException e) {
            }
        }
        String filename = String.format("/tmp/omb/producer/%s.txt",threadID.toString());
        File file = new File(filename);
        FileWriter writer = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(writer);
        for(int i=0;i<timeDelay.size();i++){
            String line = timeDelay.get(i).toString();
            bw.write(line);
            bw.newLine();
        }
        bw.close();
    }

    private void flush(Statement stmt, String table, List<String> values) throws SQLException {
        StringBuilder sb = new StringBuilder("insert into ").append(table).append(" values");
        for (String value : values) {
            sb.append(value);
        }
        String q = sb.toString();
        values.clear();
        stmt.executeUpdate(q);
    }

    public void close() {
        this.closing = true;
    }
}
