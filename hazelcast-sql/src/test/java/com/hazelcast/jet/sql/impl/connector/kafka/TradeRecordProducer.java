/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.kafka;

import org.junit.Test;

public class TradeRecordProducer {

    public TradeRecordProducer() {
    }

    /* function returns a list of SQL json-flat records in format
            (<int>, <varchar>, <int> )(,)
            where first column serves as a time source,
            second is a string value used for COUNT
            third is used for agg. operations  i.e MIN,MAX,AVG,SUM
     */

    public static String produceTradeRecords(int startTime, int count, int timeInterval, int lagTime) {
        StringBuilder sb = new StringBuilder();

        for (int i = startTime; i < startTime + count; i = i + timeInterval) {
            createSingleRecord(sb, i).append(",");
        }

        // next append evicting event
        int j = startTime + count + lagTime;
        createSingleRecord(sb, j).append(",");

        // append late event
        int k = j - 2*lagTime;
        createSingleRecord(sb, k);

        return sb.toString();
    }

    private static StringBuilder createSingleRecord(StringBuilder sb, int a) {
        sb.append("(")
                .append(a + ",")
                .append("'" + "value-" + a + "'" + ",")
                .append(a)
                .append(")");
        return sb;
    }

    @Test
    public void checkQueryString()  {
        String result = produceTradeRecords(0, 10, 1, 2);
        System.out.println(result);
        }

    @Test
    public void checkQueryString2()  {
        String result = produceTradeRecords(10, 10, 1, 2);
        System.out.println(result);
    }

    @Test
    public void checkQueryString3()  {
        String result = produceTradeRecords(20, 10, 1, 2);
        System.out.println(result);
    }

}
