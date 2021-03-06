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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

@RunWith(EasyMockRunner.class)
public class KeyValueStoreBuilderTest {

    @Mock(type = MockType.NICE)
    private KeyValueBytesStoreSupplier supplier;
    @Mock(type = MockType.NICE)
    private KeyValueStore<Bytes, byte[]> inner;
    private KeyValueStoreBuilder<String, String> builder;

    @Before
    public void setUp() throws Exception {
        EasyMock.expect(supplier.get()).andReturn(inner);
        EasyMock.expect(supplier.name()).andReturn("name");
        EasyMock.replay(supplier);
        builder = new KeyValueStoreBuilder<>(supplier,
                                             Serdes.String(),
                                             Serdes.String(),
                                             new MockTime()
        );

    }

    @Test
    public void shouldHaveMeteredStoreAsOuterStore() {
        final KeyValueStore<String, String> store = builder.build();
        assertThat(store, instanceOf(MeteredKeyValueBytesStore.class));
    }

    @Test
    public void shouldHaveChangeLoggingStoreByDefault() {
        final KeyValueStore<String, String> store = builder.build();
        assertThat(store, instanceOf(MeteredKeyValueBytesStore.class));
        final StateStore next = ((WrappedStateStore) store).wrappedStore();
        assertThat(next, instanceOf(ChangeLoggingKeyValueBytesStore.class));
    }

    @Test
    public void shouldNotHaveChangeLoggingStoreWhenDisabled() {
        final KeyValueStore<String, String> store = builder.withLoggingDisabled().build();
        final StateStore next = ((WrappedStateStore) store).wrappedStore();
        assertThat(next, CoreMatchers.<StateStore>equalTo(inner));
    }

    @Test
    public void shouldHaveCachingStoreWhenEnabled() {
        final KeyValueStore<String, String> store = builder.withCachingEnabled().build();
        final StateStore wrapped = ((WrappedStateStore) store).wrappedStore();
        assertThat(store, instanceOf(MeteredKeyValueBytesStore.class));
        assertThat(wrapped, instanceOf(CachingKeyValueStore.class));
    }

    @Test
    public void shouldHaveChangeLoggingStoreWhenLoggingEnabled() {
        final KeyValueStore<String, String> store = builder
                .withLoggingEnabled(Collections.<String, String>emptyMap())
                .build();
        final StateStore wrapped = ((WrappedStateStore) store).wrappedStore();
        assertThat(store, instanceOf(MeteredKeyValueBytesStore.class));
        assertThat(wrapped, instanceOf(ChangeLoggingKeyValueBytesStore.class));
        assertThat(((WrappedStateStore) wrapped).wrappedStore(), CoreMatchers.<StateStore>equalTo(inner));
    }

    @Test
    public void shouldHaveCachingAndChangeLoggingWhenBothEnabled() {
        final KeyValueStore<String, String> store = builder
                .withLoggingEnabled(Collections.<String, String>emptyMap())
                .withCachingEnabled()
                .build();
        final WrappedStateStore caching = (WrappedStateStore) ((WrappedStateStore) store).wrappedStore();
        final WrappedStateStore changeLogging = (WrappedStateStore) caching.wrappedStore();
        assertThat(store, instanceOf(MeteredKeyValueBytesStore.class));
        assertThat(caching, instanceOf(CachingKeyValueStore.class));
        assertThat(changeLogging, instanceOf(ChangeLoggingKeyValueBytesStore.class));
        assertThat(changeLogging.wrappedStore(), CoreMatchers.<StateStore>equalTo(inner));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerIfInnerIsNull() {
        new KeyValueStoreBuilder<>(null, Serdes.String(), Serdes.String(), new MockTime());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerIfKeySerdeIsNull() {
        new KeyValueStoreBuilder<>(supplier, null, Serdes.String(), new MockTime());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerIfValueSerdeIsNull() {
        new KeyValueStoreBuilder<>(supplier, Serdes.String(), null, new MockTime());
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerIfTimeIsNull() {
        new KeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowNullPointerIfMetricsScopeIsNull() {
        new KeyValueStoreBuilder<>(supplier, Serdes.String(), Serdes.String(), new MockTime());
    }

}