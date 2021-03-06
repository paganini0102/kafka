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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;

/**
 * Simple wrapper around a {@link SegmentedBytesStore} to support writing
 * updates to a changelog
 */
class ChangeLoggingSessionBytesStore extends WrappedStateStore.AbstractStateStore implements SessionStore<Bytes, byte[]> {

    private final SessionStore<Bytes, byte[]> bytesStore;
    private StoreChangeLogger<Bytes, byte[]> changeLogger;
    private StateSerdes<Bytes, byte[]> innerStateSerde;
    private String topic;

    ChangeLoggingSessionBytesStore(final SessionStore<Bytes, byte[]> bytesStore) {
        super(bytesStore);
        this.bytesStore = bytesStore;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        bytesStore.init(context, root);
        topic = ProcessorStateManager.storeChangelogTopic(
                context.applicationId(),
                bytesStore.name());
        innerStateSerde = WindowStoreUtils.getInnerStateSerde(
                topic);
        changeLogger = new StoreChangeLogger<>(
            name(),
            context,
            innerStateSerde);
    }


    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key, final long earliestSessionEndTime, final long latestSessionStartTime) {
        return bytesStore.findSessions(key, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom, final Bytes keyTo, final long earliestSessionEndTime, final long latestSessionStartTime) {
        return bytesStore.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
    }

    @Override
    public void remove(final Windowed<Bytes> sessionKey) {
        bytesStore.remove(sessionKey);
        changeLogger.logChange(SessionKeySerde.toBinary(sessionKey, innerStateSerde.keySerializer(), topic), null);
    }

    @Override
    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
        bytesStore.put(sessionKey, aggregate);
        changeLogger.logChange(SessionKeySerde.bytesToBinary(sessionKey), aggregate);

    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
        return findSessions(key, 0, Long.MAX_VALUE);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from, final Bytes to) {
        return findSessions(from, to, 0, Long.MAX_VALUE);
    }
}
