/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 一条记录发送的Future结果<br>
 * 其实现了Future<RecordMetadata>接口,都是委托给ProduceRequestResult实现<br>
 *      Future 表示异步计算的结果(提供了一些方法来检查计算是否完成，等待其完成以及检索计算结果);
 *      RecordMetadata 表示从server响应的Metadata
 */
public final class FutureRecordMetadata implements Future<RecordMetadata> {

    /**
     * 对应消息所在 RecordBatch 的 produceFuture
     */
    private final ProduceRequestResult result;
    /**
     * 对应消息在 RecordBatch 中的偏移量
     */
    private final long relativeOffset;
    private final long timestamp;
    private final long checksum;
    private final int serializedKeySize;
    private final int serializedValueSize;

    public FutureRecordMetadata(ProduceRequestResult result, long relativeOffset, long timestamp,
                                long checksum, int serializedKeySize, int serializedValueSize) {
        this.result = result;
        this.relativeOffset = relativeOffset;
        this.timestamp = timestamp;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
    }

    @Override
    public boolean cancel(boolean interrupt) {
        return false;
    }

    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
        this.result.await();
        return valueOrError();
    }

    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        boolean occurred = this.result.await(timeout, unit);
        if (!occurred)
            throw new TimeoutException("Timeout after waiting for " + TimeUnit.MILLISECONDS.convert(timeout, unit) + " ms.");
        return valueOrError();
    }

    RecordMetadata valueOrError() throws ExecutionException {
        if (this.result.error() != null)
            throw new ExecutionException(this.result.error());
        else
            return value();
    }

    RecordMetadata value() {
        return new RecordMetadata(result.topicPartition(), this.result.baseOffset(), this.relativeOffset,
                this.timestamp, this.checksum, this.serializedKeySize, this.serializedValueSize);
    }

    public long relativeOffset() {
        return this.relativeOffset;
    }

    public long timestamp() {
        return this.timestamp;
    }

    public long checksum() {
        return this.checksum;
    }

    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    public int serializedValueSize() {
        return this.serializedValueSize;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return this.result.completed();
    }

}
