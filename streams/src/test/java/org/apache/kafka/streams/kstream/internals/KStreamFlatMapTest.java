/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.test.KStreamTestDriver;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class KStreamFlatMapTest {

    private String topicName = "topic";

    @Rule
    public final KStreamTestDriver driver = new KStreamTestDriver();

    @Test
    public void testFlatMap() {
        StreamsBuilder builder = new StreamsBuilder();

        KeyValueMapper<Number, Object, Iterable<KeyValue<String, String>>> mapper =
            new KeyValueMapper<Number, Object, Iterable<KeyValue<String, String>>>() {
                @Override
                public Iterable<KeyValue<String, String>> apply(Number key, Object value) {
                    ArrayList<KeyValue<String, String>> result = new ArrayList<>();
                    for (int i = 0; i < key.intValue(); i++) {
                        result.add(KeyValue.pair(Integer.toString(key.intValue() * 10 + i), value.toString()));
                    }
                    return result;
                }
            };

        final int[] expectedKeys = {0, 1, 2, 3};

        KStream<Integer, String> stream;
        MockProcessorSupplier<String, String> processor;

        processor = new MockProcessorSupplier<>();
        stream = builder.stream(topicName, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream.flatMap(mapper).process(processor);

        driver.setUp(builder);
        for (int expectedKey : expectedKeys) {
            driver.process(topicName, expectedKey, "V" + expectedKey);
        }

        assertEquals(6, processor.processed.size());

        String[] expected = {"10:V1", "20:V2", "21:V2", "30:V3", "31:V3", "32:V3"};

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], processor.processed.get(i));
        }
    }
}
