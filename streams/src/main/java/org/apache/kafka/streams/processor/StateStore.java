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
package org.apache.kafka.streams.processor;

import org.apache.kafka.streams.errors.StreamsException;

/**
 * A storage engine for managing state maintained by a stream processor.
 * <p>
 * If the store is implemented as a persistent store, it <em>must</em> use the store name as directory name and write
 * all data into this store directory.
 * The store directory must be created with the state directory.
 * The state directory can be obtained via {@link ProcessorContext#stateDir() #stateDir()} using the
 * {@link ProcessorContext} provided via {@link #init(ProcessorContext, StateStore) init(...)}.
 * <p>
 * Using nested store directories within the state directory isolates different state stores.
 * If a state store would write into the state directory directly, it might conflict with others state stores and thus,
 * data might get corrupted and/or Streams might fail with an error.
 * Furthermore, Kafka Streams relies on using the store name as store directory name to perform internal cleanup tasks.
 * <p>
 * This interface does not specify any query capabilities, which, of course,
 * would be query engine specific. Instead it just specifies the minimum
 * functionality required to reload a storage engine from its changelog as well
 * as basic lifecycle management.
 */
public interface StateStore {

    /**
     * The name of this store.
     * @return the storage name
     */
    String name();

    /**
     * Initializes this state store
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    void init(ProcessorContext context, StateStore root);

    /**
     * Flush any cached data
     */
    void flush();

    /**
     * Close the storage engine.
     * Note that this function needs to be idempotent since it may be called
     * several times on the same state store.
     * <p>
     * Users only need to implement this function but should NEVER need to call this api explicitly
     * as it will be called by the library automatically when necessary
     */
    void close();

    /**
     * Return if the storage is persistent or not.
     *
     * @return  {@code true} if the storage is persistent&mdash;{@code false} otherwise
     */
    boolean persistent();

    /**
     * Is this store open for reading and writing
     * @return {@code true} if the store is open
     */
    boolean isOpen();
}
