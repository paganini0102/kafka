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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.BrokerNotFoundException;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class StreamsKafkaClient {

    private static final ConfigDef CONFIG = StreamsConfig.configDef()
            .withClientSslSupport()
            .withClientSaslSupport();

    public static class Config extends AbstractConfig {

        static Config fromStreamsConfig(Map<String, ?> props) {
            return new Config(props);
        }

        Config(Map<?, ?> originals) {
            super(CONFIG, originals, false);
        }

    }
    private final KafkaClient kafkaClient;
    private final List<MetricsReporter> reporters;
    private final Config streamsConfig;
    private final Logger log;
    private final Map<String, String> defaultTopicConfigs = new HashMap<>();
    private static final int MAX_INFLIGHT_REQUESTS = 100;

    StreamsKafkaClient(final Config streamsConfig,
                       final KafkaClient kafkaClient,
                       final List<MetricsReporter> reporters,
                       final LogContext log) {
        this.streamsConfig = streamsConfig;
        this.kafkaClient = kafkaClient;
        this.reporters = reporters;
        this.log = log.logger(StreamsKafkaClient.class);
        extractDefaultTopicConfigs(streamsConfig.originalsWithPrefix(StreamsConfig.TOPIC_PREFIX));
    }

    private void extractDefaultTopicConfigs(final Map<String, Object> configs) {
        for (final Map.Entry<String, Object> entry : configs.entrySet()) {
            if (entry.getValue() != null) {
                defaultTopicConfigs.put(entry.getKey(), entry.getValue().toString());
            }
        }
    }


    public static StreamsKafkaClient create(final Config streamsConfig) {
        final Time time = new SystemTime();

        final Map<String, String> metricTags = new LinkedHashMap<>();
        final String clientId = streamsConfig.getString(StreamsConfig.CLIENT_ID_CONFIG);
        metricTags.put("client-id", clientId);

        final Metadata metadata = new Metadata(streamsConfig.getLong(StreamsConfig.RETRY_BACKOFF_MS_CONFIG),
                                               streamsConfig.getLong(StreamsConfig.METADATA_MAX_AGE_CONFIG),
                                               false);
        final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(streamsConfig.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        metadata.update(Cluster.bootstrap(addresses), Collections.<String>emptySet(), time.milliseconds());

        final MetricConfig metricConfig = new MetricConfig().samples(streamsConfig.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(streamsConfig.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .tags(metricTags);
        final List<MetricsReporter> reporters = streamsConfig.getConfiguredInstances(
            ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
            MetricsReporter.class);
        // TODO: This should come from the KafkaStream
        reporters.add(new JmxReporter("kafka.admin.client"));
        final Metrics metrics = new Metrics(metricConfig, reporters, time);

        final ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(streamsConfig);
        final LogContext logContext = createLogContext(clientId);

        final Selector selector = new Selector(
                streamsConfig.getLong(StreamsConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),
                metrics,
                time,
                "kafka-client",
                channelBuilder,
                logContext);

        final KafkaClient kafkaClient = new NetworkClient(
                selector,
                metadata,
                clientId,
                MAX_INFLIGHT_REQUESTS, // a fixed large enough value will suffice
                streamsConfig.getLong(StreamsConfig.RECONNECT_BACKOFF_MS_CONFIG),
                streamsConfig.getLong(StreamsConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),
                streamsConfig.getInt(StreamsConfig.SEND_BUFFER_CONFIG),
                streamsConfig.getInt(StreamsConfig.RECEIVE_BUFFER_CONFIG),
                streamsConfig.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG),
                time,
                true,
                new ApiVersions(),
                logContext);
        return new StreamsKafkaClient(streamsConfig, kafkaClient, reporters, logContext);
    }

    private static LogContext createLogContext(String clientId) {
        return new LogContext("[StreamsKafkaClient clientId=" + clientId + "] ");
    }

    public static StreamsKafkaClient create(final Map<String, ?> props) {
        return create(Config.fromStreamsConfig(props));
    }

    public void close() {
        try {
            kafkaClient.close();
        } catch (final IOException impossible) {
            // this can actually never happen, because NetworkClient doesn't throw any exception on close()
            // we log just in case
            log.error("This error indicates a bug in the code. Please report to dev@kafka.apache.org.", impossible);
        } finally {
            for (MetricsReporter metricsReporter: this.reporters) {
                metricsReporter.close();
            }
        }
    }

    /**
     * Create a set of new topics using batch request.
     *
     * @throws BrokerNotFoundException if connecting failed within {@code request.timeout.ms}
     * @throws TimeoutException if there was no response within {@code request.timeout.ms}
     * @throws StreamsException for any other fatal error
     */
    public void createTopics(final Map<InternalTopicConfig, Integer> topicsMap,
                             final int replicationFactor,
                             final long windowChangeLogAdditionalRetention,
                             final MetadataResponse metadata) {
        final Map<String, CreateTopicsRequest.TopicDetails> topicRequestDetails = new HashMap<>();
        for (final Map.Entry<InternalTopicConfig, Integer> entry : topicsMap.entrySet()) {
            final InternalTopicConfig internalTopicConfig = entry.getKey();
            final Integer partitions = entry.getValue();
            final Properties topicProperties = internalTopicConfig.toProperties(windowChangeLogAdditionalRetention);
            final Map<String, String> topicConfig = new HashMap<>(defaultTopicConfigs);
            for (final String key : topicProperties.stringPropertyNames()) {
                topicConfig.put(key, topicProperties.getProperty(key));
            }
            final CreateTopicsRequest.TopicDetails topicDetails = new CreateTopicsRequest.TopicDetails(
                partitions,
                (short) replicationFactor,
                topicConfig);

            topicRequestDetails.put(internalTopicConfig.name(), topicDetails);
        }

        final ClientRequest clientRequest = kafkaClient.newClientRequest(
            getControllerReadyBrokerId(metadata),
            new CreateTopicsRequest.Builder(
                topicRequestDetails,
                streamsConfig.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG)),
            Time.SYSTEM.milliseconds(),
            true);
        final ClientResponse clientResponse = sendRequestSync(clientRequest);

        if (!clientResponse.hasResponse()) {
            throw new StreamsException("Empty response for client request.");
        }
        if (!(clientResponse.responseBody() instanceof CreateTopicsResponse)) {
            throw new StreamsException("Inconsistent response type for internal topic creation request. " +
                "Expected CreateTopicsResponse but received " + clientResponse.responseBody().getClass().getName());
        }
        final CreateTopicsResponse createTopicsResponse =  (CreateTopicsResponse) clientResponse.responseBody();

        for (InternalTopicConfig internalTopicConfig : topicsMap.keySet()) {
            ApiError error = createTopicsResponse.errors().get(internalTopicConfig.name());
            if (error.isFailure() && !error.is(Errors.TOPIC_ALREADY_EXISTS)) {
                throw new StreamsException("Could not create topic: " + internalTopicConfig.name() + " due to " + error.messageWithFallback());
            }
        }
    }

    /**
     *
     * @param nodes List of nodes to pick from.
     * @return The first node that is ready to accept requests.
     * @throws BrokerNotFoundException if connecting failed within {@code request.timeout.ms}
     */
    private String ensureOneNodeIsReady(final List<Node> nodes) {
        String brokerId = null;
        final long readyTimeout = Time.SYSTEM.milliseconds() + streamsConfig.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG);
        boolean foundNode = false;
        while (!foundNode && (Time.SYSTEM.milliseconds() < readyTimeout)) {
            for (Node node: nodes) {
                if (kafkaClient.ready(node, Time.SYSTEM.milliseconds())) {
                    brokerId = Integer.toString(node.id());
                    foundNode = true;
                    break;
                }
            }
            try {
                kafkaClient.poll(50, Time.SYSTEM.milliseconds());
            } catch (final RuntimeException e) {
                throw new StreamsException("Could not poll.", e);
            }
        }
        if (brokerId == null) {
            throw new BrokerNotFoundException("Could not find any available broker. " +
                "Check your StreamsConfig setting '" + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG + "'. " +
                "This error might also occur, if you try to connect to pre-0.10 brokers. " +
                "Kafka Streams requires broker version 0.10.1.x or higher.");
        }
        return brokerId;
    }

    /**
     * @return if Id of the controller node, or an exception if no controller is found or
     * controller is not ready
     * @throws BrokerNotFoundException if connecting failed within {@code request.timeout.ms}
     */
    private String getControllerReadyBrokerId(final MetadataResponse metadata) {
        return ensureOneNodeIsReady(Collections.singletonList(metadata.controller()));
    }

    /**
     * @return the Id of any broker that is ready, or an exception if no broker is ready.
     * @throws BrokerNotFoundException if connecting failed within {@code request.timeout.ms}
     */
    private String getAnyReadyBrokerId() {
        final Metadata metadata = new Metadata(
            streamsConfig.getLong(StreamsConfig.RETRY_BACKOFF_MS_CONFIG),
            streamsConfig.getLong(StreamsConfig.METADATA_MAX_AGE_CONFIG),
            false);
        final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(streamsConfig.getList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        metadata.update(Cluster.bootstrap(addresses), Collections.<String>emptySet(), Time.SYSTEM.milliseconds());

        final List<Node> nodes = metadata.fetch().nodes();
        return ensureOneNodeIsReady(nodes);
    }

    /**
     * @return the response to the request
     * @throws TimeoutException if there was no response within {@code request.timeout.ms}
     * @throws StreamsException any other fatal error
     */
    private ClientResponse sendRequestSync(final ClientRequest clientRequest) {
        try {
            kafkaClient.send(clientRequest, Time.SYSTEM.milliseconds());
        } catch (final RuntimeException e) {
            throw new StreamsException("Could not send request.", e);
        }

        // Poll for the response.
        final long responseTimeout = Time.SYSTEM.milliseconds() + streamsConfig.getInt(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG);
        while (Time.SYSTEM.milliseconds() < responseTimeout) {
            final List<ClientResponse> responseList;
            try {
                responseList = kafkaClient.poll(100, Time.SYSTEM.milliseconds());
            } catch (final RuntimeException e) {
                throw new StreamsException("Could not poll.", e);
            }
            if (!responseList.isEmpty()) {
                if (responseList.size() > 1) {
                    throw new StreamsException("Sent one request but received multiple or no responses.");
                }
                final ClientResponse response = responseList.get(0);
                if (response.requestHeader().correlationId() == clientRequest.correlationId()) {
                    return response;
                } else {
                    throw new StreamsException("Inconsistent response received from the broker "
                        + clientRequest.destination() + ", expected correlation id " + clientRequest.correlationId()
                        + ", but received " + response.requestHeader().correlationId());
                }
            }
        }

        throw new TimeoutException("Failed to get response from broker within timeout");
    }

    /**
     * Fetch the metadata for all topics.
     *
     * @throws BrokerNotFoundException if connecting failed within {@code request.timeout.ms}
     * @throws TimeoutException if there was no response within {@code request.timeout.ms}
     * @throws StreamsException for any other fatal error
     */
    public MetadataResponse fetchMetadata() {
        final ClientRequest clientRequest = kafkaClient.newClientRequest(
            getAnyReadyBrokerId(),
            MetadataRequest.Builder.allTopics(),
            Time.SYSTEM.milliseconds(),
            true);
        final ClientResponse clientResponse = sendRequestSync(clientRequest);

        if (!clientResponse.hasResponse()) {
            throw new StreamsException("Empty response for client request.");
        }
        if (!(clientResponse.responseBody() instanceof MetadataResponse)) {
            throw new StreamsException("Inconsistent response type for internal topic metadata request. " +
                "Expected MetadataResponse but received " + clientResponse.responseBody().getClass().getName());
        }
        return (MetadataResponse) clientResponse.responseBody();
    }

}
