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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordCollectorImpl implements RecordCollector {
    private final Logger log;
    private final Producer<byte[], byte[]> producer;
    private final Map<TopicPartition, Long> offsets;
    private final String logPrefix;

    private final static String LOG_MESSAGE = "Error sending record (key {} value {} timestamp {}) to topic {} due to {}; " +
        "No more records will be sent and no more offsets will be recorded for this task.";
    private final static String EXCEPTION_MESSAGE = "%sAbort sending since %s with a previous record (key %s value %s timestamp %d) to topic %s due to %s";
    private final static String PARAMETER_HINT = "\nYou can increase producer parameter `retries` and `retry.backoff.ms` to avoid this error.";
    private volatile KafkaException sendException;

    public RecordCollectorImpl(final Producer<byte[], byte[]> producer, final String streamTaskId, final LogContext logContext) {
        this.producer = producer;
        this.offsets = new HashMap<>();
        this.logPrefix = String.format("task [%s] ", streamTaskId);
        this.log = logContext.logger(getClass());
    }

    @Override
    public <K, V> void send(final String topic,
                            final K key,
                            final V value,
                            final Long timestamp,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer,
                            final StreamPartitioner<? super K, ? super V> partitioner) {
        Integer partition = null;

        if (partitioner != null) {
            final List<PartitionInfo> partitions = producer.partitionsFor(topic);
            if (partitions.size() > 0) {
                partition = partitioner.partition(key, value, partitions.size());
            } else {
                throw new StreamsException("Could not get partition information for topic '" + topic + "'." +
                    " This can happen if the topic does not exist.");
            }
        }

        send(topic, key, value, partition, timestamp, keySerializer, valueSerializer);
    }

    @Override
    public <K, V> void  send(final String topic,
                             final K key,
                             final V value,
                             final Integer partition,
                             final Long timestamp,
                             final Serializer<K> keySerializer,
                             final Serializer<V> valueSerializer) {
        checkForException();
        final byte[] keyBytes = keySerializer.serialize(topic, key);
        final byte[] valBytes = valueSerializer.serialize(topic, value);

        final ProducerRecord<byte[], byte[]> serializedRecord =
                new ProducerRecord<>(topic, partition, timestamp, keyBytes, valBytes);

        try {
            producer.send(serializedRecord, new Callback() {
                @Override
                public void onCompletion(final RecordMetadata metadata,
                                         final Exception exception) {
                    if (exception == null) {
                        if (sendException != null) {
                            return;
                        }
                        final TopicPartition tp = new TopicPartition(metadata.topic(), metadata.partition());
                        offsets.put(tp, metadata.offset());
                    } else {
                        if (sendException == null) {
                            if (exception instanceof ProducerFencedException) {
                                log.warn(LOG_MESSAGE, key, value, timestamp, topic, exception);
                                sendException = new ProducerFencedException(
                                    String.format(EXCEPTION_MESSAGE,
                                                  logPrefix,
                                                  "producer got fenced",
                                                  key,
                                                  value,
                                                  timestamp,
                                                  topic,
                                                  exception.getMessage()));
                            } else {
                                String errorLogMessage = LOG_MESSAGE;
                                String errorMessage = EXCEPTION_MESSAGE;
                                if (exception instanceof RetriableException) {
                                    errorLogMessage += PARAMETER_HINT;
                                    errorMessage += PARAMETER_HINT;
                                }
                                log.error(errorLogMessage, key, value, timestamp, topic, exception);
                                sendException = new StreamsException(
                                    String.format(errorMessage,
                                                  logPrefix,
                                                  "an error caught",
                                                  key,
                                                  value,
                                                  timestamp,
                                                  topic,
                                                  exception.getMessage()),
                                    exception);
                            }
                        }
                    }
                }
            });
        } catch (final TimeoutException e) {
            log.error("Timeout exception caught when sending record to topic {}. " +
                "This might happen if the producer cannot send data to the Kafka cluster and thus, " +
                "its internal buffer fills up. " +
                "You can increase producer parameter `max.block.ms` to increase this timeout.", topic);
            throw new StreamsException(String.format("%sFailed to send record to topic %s due to timeout.", logPrefix, topic));
        } catch (final Exception fatalException) {
            throw new StreamsException(
                String.format(EXCEPTION_MESSAGE,
                              logPrefix,
                              "an error caught",
                              key,
                              value,
                              timestamp,
                              topic,
                              fatalException.getMessage()),
                fatalException);
        }
    }

    private void checkForException()  {
        if (sendException != null) {
            throw sendException;
        }
    }

    @Override
    public void flush() {
        log.debug("Flushing producer");
        producer.flush();
        checkForException();
    }

    @Override
    public void close() {
        log.debug("Closing producer");
        producer.close();
        checkForException();
    }

    @Override
    public Map<TopicPartition, Long> offsets() {
        return offsets;
    }

    // for testing only
    Producer<byte[], byte[]> producer() {
        return producer;
    }

}
