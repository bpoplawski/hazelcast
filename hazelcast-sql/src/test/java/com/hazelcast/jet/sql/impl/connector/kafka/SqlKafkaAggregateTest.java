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
import com.hazelcast.sql.SqlRow;
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
import java.util.*;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Arrays.asList;

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
        String name = "trades";
        sqlService.execute("CREATE OR REPLACE MAPPING " + name + " ("
                + "tick INT,"
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

        String name2 = "tradesink";
        sqlService.execute("CREATE OR REPLACE MAPPING " + name2 + " ("
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

        sqlService.execute("INSERT INTO " + name + " VALUES" +
                TradeRecordProducer.produceTradeRecords(0, 0 + EVENT_WINDOW_COUNT, EVENT_TIME_INTERVAL, LAG_TIME));

        String jobFromSourceToString = "CREATE JOB myJob AS SINK INTO tradesink " +
                "SELECT window_start, window_end, COUNT(*) FROM " +
                "TABLE(TUMBLE(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(tick), " + LAG_TIME + ")))" +
                "  , DESCRIPTOR(tick)" +
                //  "  , 10" +
                " ," + EVENT_WINDOW_COUNT +
                ")) " +
                "GROUP BY window_start, window_end";


        sqlService.execute(jobFromSourceToString);
        Thread.sleep(5000);

        int currentEventStartTime = EVENT_START_TIME;
        long durationInMillis = 20000;

        // while loop with intervals
        begin = System.currentTimeMillis();
        List<Row> listOfExpectedRows = new ArrayList<>();

        Deque<Row> rows = new ArrayDeque<>();

        while (System.currentTimeMillis() - begin < durationInMillis) {

            currentEventStartTime = currentEventStartTime + EVENT_WINDOW_COUNT;
            sqlService.execute("INSERT INTO " + name + " VALUES" +
                    TradeRecordProducer.produceTradeRecords(currentEventStartTime, currentEventStartTime + EVENT_WINDOW_COUNT, EVENT_TIME_INTERVAL, LAG_TIME));

            Thread.sleep(5000);
        }

        try (SqlResult result = sqlService.execute("SELECT * FROM tradesink WHERE true;")) {
            Iterator<SqlRow> iterator = result.iterator();
            for (int i = 0; i < 10 && iterator.hasNext(); i++) {
                rows.add(new Row(iterator.next()));
            }
        }
        System.out.println("The rows are" + rows);
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
