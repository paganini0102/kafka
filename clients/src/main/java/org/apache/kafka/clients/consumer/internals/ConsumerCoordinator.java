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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manages the coordination process with the consumer coordinator.
 */
public final class ConsumerCoordinator extends AbstractCoordinator {
    private final Logger log;
    /**
     * PartitionAssignor列表。在消费者发送的JoinGroupRequest请求中包含了消费者自身支持的PartitionAssignor信息，
     * GroupCoordinator从所有消费者都支持的分配策略中选择一个，通知Leader使用此分配策略进行分区分配。此字段的值通
     * 过partition.assignment.strategy参数配置，可以配置多个。
     */
    private final List<PartitionAssignor> assignors;
    /** 记录kafka集群的元数据 */
    private final Metadata metadata;
    private final ConsumerCoordinatorMetrics sensors;
    /** 一个跟踪消费者的主题列表，分区列表和offsets的类 */
    private final SubscriptionState subscriptions;
    /** 提交offset的回调类 */
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    /** 是否开启了自动提交offset的功能 */
    private final boolean autoCommitEnabled;
    /** 自动提交的间隔时间 */
    private final int autoCommitIntervalMs;
    private final ConsumerInterceptors<?, ?> interceptors;
    /** 是否排除内部的topic列表 */
    private final boolean excludeInternalTopics;
    private final AtomicInteger pendingAsyncCommits;

    // this collection must be thread-safe because it is modified from the response handler
    // of offset commit requests, which may be invoked from the heartbeat thread
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;
    /** 是否是leader */
    private boolean isLeader = false;
    private Set<String> joinedSubscription;
    /** 用来存储Metadata的快照信息，主要用来检测Topic是否发生了分区数量的变化。*/
    private MetadataSnapshot metadataSnapshot;
    /** 也是用来存储Metadata的快照信息，不过是用来检测Partition分配的过程中有没有发生分区数量变化。*/
    private MetadataSnapshot assignmentSnapshot;
    /** 下一次自动提交的最后期限 */
    private long nextAutoCommitDeadline;

    /**
     * Initialize the coordination manager.
     */
    public ConsumerCoordinator(LogContext logContext,
                               ConsumerNetworkClient client,
                               String groupId,
                               int rebalanceTimeoutMs,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               List<PartitionAssignor> assignors,
                               Metadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs,
                               boolean autoCommitEnabled,
                               int autoCommitIntervalMs,
                               ConsumerInterceptors<?, ?> interceptors,
                               boolean excludeInternalTopics,
                               final boolean leaveGroupOnClose) {
        super(logContext,
              client,
              groupId,
              rebalanceTimeoutMs,
              sessionTimeoutMs,
              heartbeatIntervalMs,
              metrics,
              metricGrpPrefix,
              time,
              retryBackoffMs,
              leaveGroupOnClose);
        this.log = logContext.logger(ConsumerCoordinator.class);
        this.metadata = metadata;
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch());
        this.subscriptions = subscriptions;
        this.defaultOffsetCommitCallback = new DefaultOffsetCommitCallback();
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        this.assignors = assignors;
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>();
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        this.interceptors = interceptors;
        this.excludeInternalTopics = excludeInternalTopics;
        this.pendingAsyncCommits = new AtomicInteger();

        if (autoCommitEnabled)
            this.nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;

        this.metadata.requestUpdate();
        addMetadataListener();
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    @Override
    public List<ProtocolMetadata> metadata() {
        this.joinedSubscription = subscriptions.subscription();
        List<ProtocolMetadata> metadataList = new ArrayList<>();
        for (PartitionAssignor assignor : assignors) {
            Subscription subscription = assignor.subscription(joinedSubscription);
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);
            metadataList.add(new ProtocolMetadata(assignor.name(), metadata));
        }
        return metadataList;
    }

    public void updatePatternSubscription(Cluster cluster) {
        final Set<String> topicsToSubscribe = new HashSet<>();

        for (String topic : cluster.topics())
            if (subscriptions.subscribedPattern().matcher(topic).matches() &&
                    !(excludeInternalTopics && cluster.internalTopics().contains(topic)))
                topicsToSubscribe.add(topic);

        subscriptions.subscribeFromPattern(topicsToSubscribe);

        // note we still need to update the topics contained in the metadata. Although we have
        // specified that all topics should be fetched, only those set explicitly will be retained
        metadata.setTopics(subscriptions.groupSubscription());
    }

    // 给Metadata添加监听器
    private void addMetadataListener() {
        this.metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster, Set<String> unavailableTopics) {
                // if we encounter any unauthorized topics, raise an exception to the user
                if (!cluster.unauthorizedTopics().isEmpty())
                    throw new TopicAuthorizationException(new HashSet<>(cluster.unauthorizedTopics()));
                // AUTO_PATTERN模式的处理
                if (subscriptions.hasPatternSubscription())
                    updatePatternSubscription(cluster);

                // check if there are any changes to the metadata which should trigger a rebalance
                // 检测是否为AUTO_PATTERN或AUTO_TOPICS模式
                if (subscriptions.partitionsAutoAssigned()) {
                	// 创建快照
                    MetadataSnapshot snapshot = new MetadataSnapshot(subscriptions, cluster);
                    if (!snapshot.equals(metadataSnapshot)) // 比较快照
                        metadataSnapshot = snapshot; // 记录快照
                }

                if (!Collections.disjoint(metadata.topics(), unavailableTopics))
                    metadata.requestUpdate();
            }
        });
    }

    /**
     * 查找指定的分区分配策略
     * assignors为PartitionAssignor列表。在消费者发送的JoinGroupRequest请求中包含了消费者自身支持的PartitionAssignor信息，
     * GroupCoordinator从所有消费者都支持的分配策略中选择一个，通知Leader使用此分配策略进行分区分配。此字段的值通
     * 过partition.assignment.strategy参数配置，可以配置多个。
     */
    private PartitionAssignor lookupAssignor(String name) {
        for (PartitionAssignor assignor : this.assignors) {
            if (assignor.name().equals(name))
                return assignor;
        }
        return null;
    }

    // JoinGroupRequest请求完成之后，需要执行的动作
    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,
                                  String assignmentStrategy,
                                  ByteBuffer assignmentBuffer) {
        // only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
    	// 如果是leader则需监控元数据的改变
        if (!isLeader)
            assignmentSnapshot = null;

        // 根据Coordinator指定的消费组协议，获取唯一的分区分配器
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        // 获取分配结果
        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        // set the flag to refresh last committed offsets
        // 设置标记刷新最近一次提交的offset
        subscriptions.needRefreshCommits();

        // update partition assignment
        // 更新分区的分配
        subscriptions.assignFromSubscribed(assignment.partitions());

        // check if the assignment contains some topics that were not in the original
        // subscription, if yes we will obey what leader has decided and add these topics
        // into the subscriptions as long as they still match the subscribed pattern
        //
        // TODO this part of the logic should be removed once we allow regex on leader assign
        Set<String> addedTopics = new HashSet<>();
        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            if (!joinedSubscription.contains(tp.topic()))
                addedTopics.add(tp.topic());
        }

        if (!addedTopics.isEmpty()) {
            Set<String> newSubscription = new HashSet<>(subscriptions.subscription());
            Set<String> newJoinedSubscription = new HashSet<>(joinedSubscription);
            newSubscription.addAll(addedTopics);
            newJoinedSubscription.addAll(addedTopics);

            this.subscriptions.subscribeFromPattern(newSubscription);
            this.joinedSubscription = newJoinedSubscription;
        }

        // update the metadata and enforce a refresh to make sure the fetcher can start
        // fetching data in the next iteration
        this.metadata.setTopics(subscriptions.groupSubscription());
        client.ensureFreshMetadata();

        // give the assignor a chance to update internal state based on the received assignment
        // 给一个分区分配策略一个机会更新分配结果
        assignor.onAssignment(assignment);

        // reschedule the auto commit starting from now
        this.nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;

        // execute the user's callback after rebalance
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Setting newly assigned partitions {}", subscriptions.assignedPartitions());
        try {
            Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsAssigned(assigned);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on partition assignment", listener.getClass().getName(), e);
        }
    }

    /**
     * Poll for coordinator events. This ensures that the coordinator is known and that the consumer
     * has joined the group (if it is using group management). This also handles periodic offset commits
     * if they are enabled.
     *
     * @param now current time in milliseconds
     */
    public void poll(long now, long remainingMs) {
    	// 调用offset，提交请求的回调函数
        invokeCompletedOffsetCommitCallbacks();
        // 判断订阅状态是否自动分配分区
        if (subscriptions.partitionsAutoAssigned()) {
        	// coordinator为空
            if (coordinatorUnknown()) {
            	// 确保coordinator已经准备好接收请求了
                ensureCoordinatorReady();
                now = time.milliseconds();
            }
            // 判断是否需要重新join
            if (needRejoin()) {
                // due to a race condition between the initial metadata fetch and the initial rebalance,
                // we need to ensure that the metadata is fresh before joining initially. This ensures
                // that we have matched the pattern against the cluster's topics at least once before joining.
            	// 由于初始元数据获取和初始重新平衡之间的竞争条件，我们需要确保元数据在开始加入之前是最新的。
            	// 确保了我们在join之前至少有一次与集群的主题的模式匹配
                if (subscriptions.hasPatternSubscription())
                    client.ensureFreshMetadata(); // 刷新元数据
                // 确保协调器可用，心跳线程开启和组可用
                ensureActiveGroup();
                now = time.milliseconds();	
            }
        } else {
            // For manually assigned partitions, if there are no ready nodes, await metadata.
            // If connections to all nodes fail, wakeups triggered while attempting to send fetch
            // requests result in polls returning immediately, causing a tight loop of polls. Without
            // the wakeup, poll() with no channels would block for the timeout, delaying re-connection.
            // awaitMetadataUpdate() initiates new connections with configured backoff and avoids the busy loop.
            // When group management is used, metadata wait is already performed for this scenario as
            // coordinator is unknown, hence this check is not required.
            if (metadata.updateRequested() && !client.hasReadyNodes()) {
                boolean metadataUpdated = client.awaitMetadataUpdate(remainingMs);
                if (!metadataUpdated && !client.hasReadyNodes())
                    return;
                now = time.milliseconds();
            }
        }
        // 检测心跳线程的状态
        pollHeartbeat(now);
        // 异步的自动提交offset
        maybeAutoCommitOffsetsAsync(now);
    }

    /**
     * Return the time to the next needed invocation of {@link #poll(long)}.
     * @param now current time in milliseconds
     * @return the maximum time in milliseconds the caller should wait before the next invocation of poll()
     */
    public long timeToNextPoll(long now) {
        if (!autoCommitEnabled)
            return timeToNextHeartbeat(now);

        if (now > nextAutoCommitDeadline)
            return 0;

        return Math.min(nextAutoCommitDeadline - now, timeToNextHeartbeat(now));
    }

    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                        String assignmentStrategy,
                                                        Map<String, ByteBuffer> allSubscriptions) {
    	// 查找分区分配使用的PartitionAssignor
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        Set<String> allSubscribedTopics = new HashSet<>();
        Map<String, Subscription> subscriptions = new HashMap<>();
        // 根据JoinGroupResponse的分组信息(group_protocols)对Subscription和topic分类汇总
        for (Map.Entry<String, ByteBuffer> subscriptionEntry : allSubscriptions.entrySet()) {
            Subscription subscription = ConsumerProtocol.deserializeSubscription(subscriptionEntry.getValue());
            subscriptions.put(subscriptionEntry.getKey(), subscription);
            allSubscribedTopics.addAll(subscription.topics());
        }

        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        // leader会获取所有组内所有消费者的订阅的topic
        this.subscriptions.groupSubscribe(allSubscribedTopics);
        metadata.setTopics(this.subscriptions.groupSubscription());

        // update metadata (if needed) and keep track of the metadata used for assignment so that
        // we can check after rebalance completion whether anything has changed
        // 更新metadata
        client.ensureFreshMetadata();

        isLeader = true;

        log.debug("Performing assignment using strategy {} with subscriptions {}", assignor.name(), subscriptions);

        // 进行分区分配
        Map<String, Assignment> assignment = assignor.assign(metadata.fetch(), subscriptions);

        // user-customized assignor may have created some topics that are not in the subscription list
        // and assign their partitions to the members; in this case we would like to update the leader's
        // own metadata with the newly added topics so that it will not trigger a subsequent rebalance
        // when these topics gets updated from metadata refresh.
        //
        // TODO: this is a hack and not something we want to support long-term unless we push regex into the protocol
        //       we may need to modify the PartitionAssignor API to better support this case.
        Set<String> assignedTopics = new HashSet<>();
        for (Assignment assigned : assignment.values()) {
            for (TopicPartition tp : assigned.partitions())
                assignedTopics.add(tp.topic());
        }

        if (!assignedTopics.containsAll(allSubscribedTopics)) {
            Set<String> notAssignedTopics = new HashSet<>(allSubscribedTopics);
            notAssignedTopics.removeAll(assignedTopics);
            log.warn("The following subscribed topics are not assigned to any members: {} ", notAssignedTopics);
        }

        if (!allSubscribedTopics.containsAll(assignedTopics)) {
            Set<String> newlyAddedTopics = new HashSet<>(assignedTopics);
            newlyAddedTopics.removeAll(allSubscribedTopics);
            log.info("The following not-subscribed topics are assigned, and their metadata will be " +
                    "fetched from the brokers: {}", newlyAddedTopics);

            allSubscribedTopics.addAll(assignedTopics);
            this.subscriptions.groupSubscribe(allSubscribedTopics);
            metadata.setTopics(this.subscriptions.groupSubscription());
            client.ensureFreshMetadata();
        }
        // 记录快照
        assignmentSnapshot = metadataSnapshot;

        log.debug("Finished assignment for group: {}", assignment);

        // 分区分配结果序列化并保存到到groupAssignment中，返回分区结果
        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignment.entrySet()) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        // commit offsets prior to rebalance if auto-commit enabled
    	// 如果开启了自动提交，那就在Rebalance之前提交offset
        maybeAutoCommitOffsetsSync(rebalanceTimeoutMs);

        // execute the user's callback before rebalance
        // 执行注册在SubscriptionState上的ConsumerRebalanceListener回调方法
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Revoking previously assigned partitions {}", subscriptions.assignedPartitions());
        try {
            Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsRevoked(revoked);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} failed on partition revocation", listener.getClass().getName(), e);
        }

        isLeader = false;
        // 重置该组的订阅，只包含该用户订阅的主题
        subscriptions.resetGroupSubscription();
    }

    @Override
    public boolean needRejoin() {
    	// 检测Consumer的订阅是否为自动分配模式，用户分配不需要进行Rebalance
        if (!subscriptions.partitionsAutoAssigned())
            return false;

        // we need to rejoin if we performed the assignment and metadata has changed
        // 如果元数据发生了改变，需要Rejoin
        if (assignmentSnapshot != null && !assignmentSnapshot.equals(metadataSnapshot))
            return true;

        // we need to join if our subscription has changed since the last join
        // 如果我们的订阅自上一次join之后改变了，需要Rejoin
        if (joinedSubscription != null && !joinedSubscription.equals(subscriptions.subscription()))
            return true;
        // 设置重新Rejoin
        return super.needRejoin();
    }

    /**
     * Refresh the committed offsets for provided partitions.
     * 更新分区状态中的已提交偏移量
     */
    public void refreshCommittedOffsetsIfNeeded() {
        if (subscriptions.refreshCommitsNeeded()) {
        	// 对于分配的partition,从协调器中获取当前已提交的偏移量
            Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(subscriptions.assignedPartitions());
            // 遍历TopicPartition
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                // verify assignment is still active
                if (subscriptions.isAssigned(tp))
                	// 更新分区状态的committed变量，协调节点保存的数据更新到客户端
                    this.subscriptions.committed(tp, entry.getValue());
            }
            // 刷新提交
            this.subscriptions.commitsRefreshed();
        }
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(Set<TopicPartition> partitions) {
        while (true) {
            ensureCoordinatorReady();

            // contact coordinator to fetch committed offsets
            RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future = sendOffsetFetchRequest(partitions);
            client.poll(future);

            if (future.succeeded())
                return future.value();

            if (!future.isRetriable())
                throw future.exception();

            time.sleep(retryBackoffMs);
        }
    }

    public void close(long timeoutMs) {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();

        long now = time.milliseconds();
        long endTimeMs = now + timeoutMs;
        try {
            maybeAutoCommitOffsetsSync(timeoutMs);
            now = time.milliseconds();
            if (pendingAsyncCommits.get() > 0 && endTimeMs > now) {
                ensureCoordinatorReady(now, endTimeMs - now);
                now = time.milliseconds();
            }
        } finally {
            super.close(Math.max(0, endTimeMs - now));
        }
    }

    // visible for testing
    void invokeCompletedOffsetCommitCallbacks() {
        while (true) {
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null)
                break;
            completion.invoke();
        }
    }

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
    	// 调用offset，提交请求的回调函数
        invokeCompletedOffsetCommitCallbacks();

        if (!coordinatorUnknown()) {
            doCommitOffsetsAsync(offsets, callback);
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit
            // or fail (we don't want recursive retries which can cause offset commits to arrive
            // out of order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in
            // the same order that they were added. Note also that AbstractCoordinator prevents
            // multiple concurrent coordinator lookup requests.
            pendingAsyncCommits.incrementAndGet();
            lookupCoordinator().addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    doCommitOffsetsAsync(offsets, callback);
                }

                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets,
                            RetriableCommitFailedException.withUnderlyingMessage(e.getMessage())));
                }
            });
        }

        // ensure the commit has a chance to be transmitted (without blocking on its completion).
        // Note that commits are treated as heartbeats by the coordinator, so there is no need to
        // explicitly allow heartbeats through delayed task execution.
        client.pollNoWakeup();
    }

    private void doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        this.subscriptions.needRefreshCommits();
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);

                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
            }

            @Override
            public void onFailure(RuntimeException e) {
                Exception commitException = e;

                if (e instanceof RetriableException)
                    commitException = RetriableCommitFailedException.withUnderlyingMessage(e.getMessage());

                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, commitException));
            }
        });
    }

    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     * @param offsets The offsets to be committed
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *             or to any of the specified partitions. See the exception for more details
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be completed
     * @return If the offset commit was successfully sent and a successful response was received from
     *         the coordinator
     */
    public boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, long timeoutMs) {
        invokeCompletedOffsetCommitCallbacks();

        if (offsets.isEmpty())
            return true;

        long now = time.milliseconds();
        long startMs = now;
        long remainingMs = timeoutMs;
        do {
            if (coordinatorUnknown()) {
                if (!ensureCoordinatorReady(now, remainingMs))
                    return false;

                remainingMs = timeoutMs - (time.milliseconds() - startMs);
            }

            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future, remainingMs);

            // We may have had in-flight offset commits when the synchronous commit began. If so, ensure that
            // the corresponding callbacks are invoked prior to returning in order to preserve the order that
            // the offset commits were applied.
            invokeCompletedOffsetCommitCallbacks();

            if (future.succeeded()) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                return true;
            }

            if (future.failed() && !future.isRetriable())
                throw future.exception();

            time.sleep(retryBackoffMs);

            now = time.milliseconds();
            remainingMs = timeoutMs - (now - startMs);
        } while (remainingMs > 0);

        return false;
    }

    private void maybeAutoCommitOffsetsAsync(long now) {
        if (autoCommitEnabled) {
            if (coordinatorUnknown()) {
                this.nextAutoCommitDeadline = now + retryBackoffMs;
            } else if (now >= nextAutoCommitDeadline) {
                this.nextAutoCommitDeadline = now + autoCommitIntervalMs;
                doAutoCommitOffsetsAsync();
            }
        }
    }

    public void maybeAutoCommitOffsetsNow() {
        if (autoCommitEnabled && !coordinatorUnknown())
            doAutoCommitOffsetsAsync();
    }

    private void doAutoCommitOffsetsAsync() {
        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
        log.debug("Sending asynchronous auto-commit of offsets {}", allConsumedOffsets);

        commitOffsetsAsync(allConsumedOffsets, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception != null) {
                    log.warn("Asynchronous auto-commit of offsets {} failed: {}", offsets, exception.getMessage());
                    if (exception instanceof RetriableException)
                        nextAutoCommitDeadline = Math.min(time.milliseconds() + retryBackoffMs, nextAutoCommitDeadline);
                } else {
                    log.debug("Completed asynchronous auto-commit of offsets {}", offsets);
                }
            }
        });
    }

    private void maybeAutoCommitOffsetsSync(long timeoutMs) {
        if (autoCommitEnabled) {
            Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
            try {
                log.debug("Sending synchronous auto-commit of offsets {}", allConsumedOffsets);
                if (!commitOffsetsSync(allConsumedOffsets, timeoutMs))
                    log.debug("Auto-commit of offsets {} timed out before completion", allConsumedOffsets);
            } catch (WakeupException | InterruptException e) {
                log.debug("Auto-commit of offsets {} was interrupted before completion", allConsumedOffsets);
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Synchronous auto-commit of offsets {} failed: {}", allConsumedOffsets, e.getMessage());
            }
        }
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    private RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        Node coordinator = coordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        // create the offset commit request
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>(offsets.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata.offset() < 0) {
                return RequestFuture.failure(new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset()));
            }
            offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(
                    offsetAndMetadata.offset(), offsetAndMetadata.metadata()));
        }

        final Generation generation;
        if (subscriptions.partitionsAutoAssigned())
            generation = generation();
        else
            generation = Generation.NO_GENERATION;

        // if the generation is null, we are not part of an active group (and we expect to be).
        // the only thing we can do is fail the commit and let the user rejoin the group in poll()
        if (generation == null)
            return RequestFuture.failure(new CommitFailedException());

        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(this.groupId, offsetData).
                setGenerationId(generation.generationId).
                setMemberId(generation.memberId).
                setRetentionTime(OffsetCommitRequest.DEFAULT_RETENTION_TIME);

        log.trace("Sending OffsetCommit request with {} to coordinator {}", offsets, coordinator);

        return client.send(coordinator, builder)
                .compose(new OffsetCommitResponseHandler(offsets));
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitLatency.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            for (Map.Entry<TopicPartition, Errors> entry : commitResponse.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);
                long offset = offsetAndMetadata.offset();

                Errors error = entry.getValue();
                if (error == Errors.NONE) {
                    log.debug("Committed offset {} for partition {}", offset, tp);
                    if (subscriptions.isAssigned(tp))
                        // update the local cache only if the partition is still assigned
                        subscriptions.committed(tp, offsetAndMetadata);
                } else {
                    log.error("Offset commit failed on partition {} at offset {}: {}", tp, offset, error.message());

                    if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                        future.raise(new GroupAuthorizationException(groupId));
                        return;
                    } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                        unauthorizedTopics.add(tp.topic());
                    } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                            || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                        // raise the error to the user
                        future.raise(error);
                        return;
                    } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                        // just retry
                        future.raise(error);
                        return;
                    } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                            || error == Errors.NOT_COORDINATOR
                            || error == Errors.REQUEST_TIMED_OUT) {
                        coordinatorDead();
                        future.raise(error);
                        return;
                    } else if (error == Errors.UNKNOWN_MEMBER_ID
                            || error == Errors.ILLEGAL_GENERATION
                            || error == Errors.REBALANCE_IN_PROGRESS) {
                        // need to re-join group
                        resetGeneration();
                        future.raise(new CommitFailedException());
                        return;
                    } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(new KafkaException("Topic or Partition " + tp + " does not exist"));
                        return;
                    } else {
                        future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                        return;
                    }
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {}", unauthorizedTopics);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        Node coordinator = coordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Fetching committed offsets for partitions: {}", partitions);
        // construct the request
        OffsetFetchRequest.Builder requestBuilder = new OffsetFetchRequest.Builder(this.groupId,
                new ArrayList<>(partitions));

        // send the request with a callback
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {
        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            if (response.hasError()) {
                Errors error = response.error();
                log.debug("Offset fetch failed: {}", error.message());

                if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                    // just retry
                    future.raise(error);
                } else if (error == Errors.NOT_COORDINATOR) {
                    // re-discover the coordinator and retry
                    coordinatorDead();
                    future.raise(error);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(new GroupAuthorizationException(groupId));
                } else {
                    future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                }
                return;
            }

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                if (data.hasError()) {
                    Errors error = data.error;
                    log.debug("Failed to fetch offset for partition {}: {}", tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(new KafkaException("Topic or Partition " + tp + " does not exist"));
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                    }
                    return;
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.metadata));
                } else {
                    log.debug("Found no committed offset for partition {}", tp);
                }
            }

            future.complete(offsets);
        }
    }

    private class ConsumerCoordinatorMetrics {
        private final String metricGrpName;
        private final Sensor commitLatency;

        private ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitLatency.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitLatency.add(createMeter(metrics, metricGrpName, "commit", "commit calls"));

            Measurable numParts =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return subscriptions.assignedPartitions().size();
                    }
                };
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class MetadataSnapshot {
        private final Map<String, Integer> partitionsPerTopic;

        private MetadataSnapshot(SubscriptionState subscription, Cluster cluster) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.groupSubscription())
                partitionsPerTopic.put(topic, cluster.partitionCountForTopic(topic));
            this.partitionsPerTopic = partitionsPerTopic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetadataSnapshot that = (MetadataSnapshot) o;
            return partitionsPerTopic != null ? partitionsPerTopic.equals(that.partitionsPerTopic) : that.partitionsPerTopic == null;
        }

        @Override
        public int hashCode() {
            return partitionsPerTopic != null ? partitionsPerTopic.hashCode() : 0;
        }
    }

    private static class OffsetCommitCompletion {
        private final OffsetCommitCallback callback;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Exception exception;

        private OffsetCommitCompletion(OffsetCommitCallback callback, Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            this.callback = callback;
            this.offsets = offsets;
            this.exception = exception;
        }

        public void invoke() {
            if (callback != null)
                callback.onComplete(offsets, exception);
        }
    }

}
