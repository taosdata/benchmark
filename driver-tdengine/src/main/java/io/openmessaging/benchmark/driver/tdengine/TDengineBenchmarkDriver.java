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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class TDengineBenchmarkDriver implements BenchmarkDriver {
    private boolean initialized = false;
    private AtomicInteger createdProducers = new AtomicInteger(-1);
    // SubTables belong to different VGroups;
    private List<String> prepSubTables = new ArrayList<>();
    private Config config;
    private Connection conn;

    @Override
    public void initialize(File configurationFile, StatsLogger statsLogger) throws IOException {
        config = mapper.readValue(configurationFile, Config.class);

        try {
            conn = DriverManager.getConnection(config.jdbcURL);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (Statement stmt = conn.createStatement()) {
            String q = "drop database if exists " + config.database;
            log.info(q);
            stmt.executeUpdate(q);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getTopicNamePrefix() {
        return "td";
    }

    /**
     * Create database.
     *
     * @param stmt
     * @param partitions
     * @throws SQLException
     */
    public void myInitialize(Statement stmt, int partitions) throws SQLException {
        if (initialized) {
            return;
        }
        String q = "create database if not exists " + config.database
                + " precision 'ns'"
                + " vgroups " + partitions
                + " replica " + config.replica
                + " wal_level " + config.walLevel
                + " wal_fsync_period " + config.walFsyncPeriod
                + " wal_retention_period " + config.walRetentionPeriod
                + " wal_retention_size " + config.walRetentionSize
                + " wal_roll_period " + config.walRollPeriod
                + " wal_seg_size " + config.walSegSize
                + " strict '" + config.strict + "'";
        log.info(q);
        stmt.executeUpdate(q);
        initialized = true;
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        CompletableFuture future = new CompletableFuture();
        try (Statement stmt = conn.createStatement()) {
            myInitialize(stmt, partitions);
            stmt.executeUpdate("use " + config.database);
            String stable = topic.replaceAll("-", "_");
            String q = "create stable if not exists " + stable + "(ts timestamp, payload binary(" + config.varcharLen + ")) tags(id bigint)";
            log.info(q);
            stmt.executeUpdate(q);
            createSubTables(stmt, stable, partitions);
            setPrepSubTables(stmt);
            q = "create topic `" + topic + "` as stable " + stable;
            log.info(q);
            stmt.executeUpdate(q);
            future.complete(null);
        } catch (SQLException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private void setPrepSubTables(Statement stmt) throws SQLException {
        String q = "select first(table_name), vgroup_id, db_name from ins_tables group by vgroup_id, db_name having db_name='" + config.database+ "'";
        log.info(q);
        ResultSet rs = stmt.executeQuery(q);
        while (rs.next()) {
            prepSubTables.add(rs.getString(1));
        }
    }

    /**
     * Create SubTables. The number of SubTables creates is 10 * (number of partitions).
     * But only one SubTable will be used for each partition.
     * @param stmt
     * @param stable
     * @param partitions
     * @throws SQLException
     */
    private void createSubTables(Statement stmt, String stable, int partitions) throws SQLException {
        for (int i = 0; i < partitions * 10; ++i) {
            String tableName = (stable + "_" + i).toLowerCase();
            String q = "create table " + tableName + " using " + stable + " tags(" + i + ")";
            log.info(q);
            stmt.executeUpdate(q);
        }
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        int producerId = createdProducers.incrementAndGet();
        String tableName = prepSubTables.get(producerId % (prepSubTables.size()));
        log.info("create TDengineProducer {} with tableName={}", producerId, tableName);
        return CompletableFuture.completedFuture(new TDengineBenchmarkProducer(new TDengineProducer(topic, tableName, config)));
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        return CompletableFuture.completedFuture(new TDengineBenchmarkConsumer(topic, subscriptionName, consumerCallback));
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TDengineBenchmarkDriver.class);
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
}
