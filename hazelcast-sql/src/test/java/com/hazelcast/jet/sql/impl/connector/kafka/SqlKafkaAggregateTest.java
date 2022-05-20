/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.jet.kafka.impl.KafkaTestSupport;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlKafkaAggregateTest extends SqlTestSupport {

    private static final int INITIAL_PARTITION_COUNT = 1;
    private static KafkaTestSupport kafkaTestSupport;

    private final static int EVENT_START_TIME = 0;
    private final static int EVENT_WINDOW_COUNT = 10;
    private final static int EVENT_TIME_INTERVAL = 1;
    private final static int LAG_TIME = 2;

    private long begin;

    private static SqlService sqlService;

    private final static String SOURCE = "trades";
    private final static String SINK = "tradesink";

    @BeforeClass
    public static void setUpClass() throws IOException {
        initialize(1, null);
        sqlService = instance().getSql();

        kafkaTestSupport = KafkaTestSupport.create();
        kafkaTestSupport.createKafkaCluster();
    }

    @AfterClass
    public static void tearDownClass() {
        kafkaTestSupport.shutdownKafkaCluster();
    }

    @Test
    public void test_tumble() throws InterruptedException {
        // sqlService = client.getSql();

        sqlService.execute("CREATE MAPPING " + SOURCE + " ("
                + "tick BIGINT,"
                + "ticker VARCHAR,"
                + "price DECIMAL" + ") "
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='json-flat'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );

        sqlService.execute("CREATE OR REPLACE MAPPING " + SINK + " ("
                + "__key INT,"
                + "windowsend INT,"
                + "countsc INT"
                + ") "
                + "TYPE " + IMapSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='json-flat'"
                + ")"
        );

        sqlService.execute("INSERT INTO " + SOURCE + " VALUES" +
                TradeRecordProducer.produceTradeRecords(EVENT_START_TIME, EVENT_WINDOW_COUNT, EVENT_TIME_INTERVAL, LAG_TIME));
        //Thread.sleep(5000);

        String jobFromSourceToString = "CREATE JOB myJob AS SINK INTO " + SINK +
                " SELECT window_start, window_end, AVG(price) FROM " +
                "TABLE(TUMBLE(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + SOURCE + ", DESCRIPTOR(tick), " + LAG_TIME + ")))" +
                "  , DESCRIPTOR(tick)" +
                " ," + EVENT_WINDOW_COUNT +
                ")) " +
                "GROUP BY window_start, window_end";

        sqlService.execute(jobFromSourceToString);
        Thread.sleep(5000);

        long durationInMillis = 30000;

        // initiate counters
        String sinkRowSqlQuery;
        int currentEventStartTime = EVENT_START_TIME;
        int currentEventEndTime;
        begin = System.currentTimeMillis();

        ExecutorService threadPool = Executors.newSingleThreadExecutor();



        while (System.currentTimeMillis() - begin < durationInMillis) {

            // ingest data to Kafka using new timestamps
            currentEventStartTime = currentEventStartTime + EVENT_WINDOW_COUNT;
            currentEventEndTime = currentEventStartTime + EVENT_WINDOW_COUNT;
            sqlService.execute("INSERT INTO " + "trades" + " VALUES" +
                    TradeRecordProducer.produceTradeRecords(currentEventStartTime, currentEventStartTime +
                            EVENT_WINDOW_COUNT, EVENT_TIME_INTERVAL, LAG_TIME));

            // timeout between queries to not stress out the cluster
            Thread.sleep(100);

            // prepare sink row query
            sinkRowSqlQuery = "SELECT countsc FROM " + SINK + " WHERE __key=" + currentEventStartTime +
                    " AND windowsend=" + currentEventEndTime;

            // execute query with wait
            SqlResultProcessor sqlResultProcessor = new SqlResultProcessor(sinkRowSqlQuery, sqlService, threadPool);
            SqlResult sqlResult = null;
            Future<SqlResult> sqlResultFuture = sqlResultProcessor.runQueryAsync();
            sqlResult = sqlResultProcessor.awaitQueryExecutionWithTimeout(sqlResultFuture, 10);

            // assert query successful
            assertQuerySuccessful(sqlResult, currentEventStartTime, currentEventEndTime);

            //currentQueryCount++;

            // print progress
            //printProgress();
        }

    }

    private void assertQuerySuccessful(SqlResult actualSqlResult, int currentEventStartTime, int currentEventEndTime) {
        int actualAvgValue = actualSqlResult.iterator().next().getObject(0);
        int expectedValue = (int) IntStream.range(currentEventStartTime, currentEventEndTime).average().getAsDouble();
        assertEquals("The avg count over aggregate window does not match", expectedValue, actualAvgValue);
        System.out.println("Matching for event time " + currentEventStartTime);

    }

    @Test
    public void test_hop() {
        String name = createRandomTopic();
        sqlService.execute("CREATE MAPPING " + name + ' '
                + "TYPE " + KafkaSqlConnector.TYPE_NAME + ' '
                + "OPTIONS ( "
                + '\'' + OPTION_KEY_FORMAT + "'='int'"
                + ", '" + OPTION_VALUE_FORMAT + "'='varchar'"
                + ", 'bootstrap.servers'='" + kafkaTestSupport.getBrokerConnectionString() + '\''
                + ", 'auto.offset.reset'='earliest'"
                + ")"
        );
        sqlService.execute("INSERT INTO " + name + " VALUES" +
                "(0, 'value-0')" +
                ", (1, 'value-1')" +
                ", (2, 'value-2')" +
                ", (10, 'value-10')"
        );

        assertTipOfStream(
                "SELECT window_start, window_end, SUM(__key) FROM " +
                        "TABLE(HOP(" +
                        "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(__key), 2))), " +
                        "DESCRIPTOR(__key), 4, 2)) " +
                        "GROUP BY window_start, window_end",
                asList(
                        new Row(-2, 2, 1L),
                        new Row(0, 4, 3L),
                        new Row(2, 6, 2L)
                )
        );
    }

    private static String createRandomTopic() {
        String topicName = randomName();
        kafkaTestSupport.createTopic(topicName, INITIAL_PARTITION_COUNT);
        return topicName;
    }
}
