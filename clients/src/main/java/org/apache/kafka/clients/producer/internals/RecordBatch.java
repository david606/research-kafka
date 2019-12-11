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

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A batch of records that is or will be sent.<br>
 * 一批即将或将要发送的记录 (此类不是线程安全的，修改该类时必须使用外部同步)
 */
public final class RecordBatch {

    private static final Logger log = LoggerFactory.getLogger(RecordBatch.class);

    /**
     * 记录了保存的 Record 的个数
     */
    public int recordCount = 0;
    /**
     * 最大 Record 的字节数
     */
    public int maxRecordSize = 0;
    /**
     * 尝试发送当前 RecordBatch 的次数
     */
    public volatile int attempts = 0;
    public final long createdMs;
    public long drainedMs;
    /**
     * 最后一次尝试发送的时间戳
     */
    public long lastAttemptMs;
    /**
     * 指向用来存储数据的 MemoryRecords 对象
     */
    public final MemoryRecords records;
    /**
     * 当前 RecordBatch 中缓存的消息都会发送给此 TopicPartition
     */
    public final TopicPartition topicPartition;
    /**
     * 标识 RecordBatch 状态的 Future 对象
     */
    public final ProduceRequestResult produceFuture;
    /**
     * 最后一次向 RecordBatch 追加消息的时间戳
     */
    public long lastAppendTime;
    /**
     * 消息的回调对象队列:Thunk(封装Callback和与其关联的FutureRecordMetadata)
     */
    private final List<Thunk> thunks;
    /**
     * 用来记录某消息在 RecordBatch 中的偏移量
     */
    private long offsetCounter = 0L;
    /**
     * 是否正在重试<br>
     * 如果 RecordBatch 中的数据发送失败 , 则会重新尝试发送
     */
    private boolean retry;

    public RecordBatch(TopicPartition tp, MemoryRecords records, long now) {
        this.createdMs = now;
        this.lastAttemptMs = now;
        this.records = records;
        this.topicPartition = tp;
        this.produceFuture = new ProduceRequestResult();
        this.thunks = new ArrayList<Thunk>();
        this.lastAppendTime = createdMs;
        this.retry = false;
    }

    /**
     * 将记录追加到当前记录集，然后返回该记录集内的相对偏移量
     *
     * @return 与此记录对应的RecordSend；如果没有足够的空间，则为null。
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, long now) {
        //估算剩余空间是否足够容纳新的记录(这不是一个准确值)
        if (!this.records.hasRoomFor(key, value)) {
            return null;
        } else {
            //向 MemoryRecords 中添加数据.注意 , offsetCounter 是在 RecordBatch 中的偏移量
            long checksum = this.records.append(offsetCounter++, timestamp, key, value);
            //更新统计信息
            this.maxRecordSize = Math.max(this.maxRecordSize, Record.recordSize(key, value));
            this.lastAppendTime = now;
            //创建 FutureRecordMetadata 对象
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture,//真正实现Future接口的是produceFuture
                    this.recordCount,
                    timestamp, checksum,
                    key == null ? -1 : key.length,
                    value == null ? -1 : value.length);
            //将用户自定义 CallBack 和 FutureRecordMetadata 封装成 Thunk , 保存到 thunks 集合中
            if (callback != null)
                thunks.add(new Thunk(callback, future));
            this.recordCount++;
            return future;
        }
    }

    /**
     * 请求完成
     *       当RecordBatch 成功收到正常响应 、 或超时 、 或关闭生产者时 , 都会调用此方法
     *
     * @param baseOffset The base offset of the messages assigned by the server
     * @param timestamp  The timestamp returned by the broker.
     * @param exception  The exception that occurred (or null if the request was successful)
     */
    public void done(long baseOffset, long timestamp, RuntimeException exception) {
        log.trace("Produced messages to topic-partition {} with base offset offset {} and error: {}.",
                topicPartition,
                baseOffset,
                exception);
        // execute callbacks
        for (int i = 0; i < this.thunks.size(); i++) {
            try {
                Thunk thunk = this.thunks.get(i);
                //无异常,说明发送成功
                if (exception == null) {
                    //将服务端返回的信息( offset 和 timestamp ) 和消息的其他信息封装成 RecordMetadata
                    RecordMetadata metadata = new RecordMetadata(this.topicPartition, baseOffset, thunk.future.relativeOffset(),
                            //如果服务器返回的时间戳为NoTimestamp，则表示使用CreateTime。否则，将使用LogAppendTime
                            timestamp == Record.NO_TIMESTAMP ? thunk.future.timestamp() : timestamp,
                            thunk.future.checksum(),
                            thunk.future.serializedKeySize(),
                            thunk.future.serializedValueSize());

                    //调用消息对应的自定义 Callback
                    thunk.callback.onCompletion(metadata, null);

                } else {//处理过程中出现异常的情况 , 注意 , 第一个参数为 null , 与上面正常的情况相反
                    thunk.callback.onCompletion(null, exception);
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition {}:", topicPartition, e);
            }
        }
        //标识整个 RecordBatch 都已经处理完成
        this.produceFuture.done(topicPartition, baseOffset, exception);
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        public Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "RecordBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * A batch whose metadata is not available should be expired if one of the following is true:
     * <ol>
     * <li> 该批次不处于重试状态，并且准备就绪后，请求已超时(full or (徘徊)linger.ms has reached).
     * <li> 该批次正在重试，并且退避期结束后，请求已超时.
     * </ol>
     */
    public boolean maybeExpire(int requestTimeoutMs, long retryBackoffMs, long now, long lingerMs, boolean isFull) {
        boolean expire = false;
        String errorMessage = null;

        //未处理于重试状态,且请求超时
        if (!this.inRetry() && isFull && requestTimeoutMs < (now - this.lastAppendTime)) {
            expire = true;
            errorMessage = (now - this.lastAppendTime) + " ms has passed since last append";
        } else if (!this.inRetry() && requestTimeoutMs < (now - (this.createdMs + lingerMs))) {
            expire = true;
            errorMessage = (now - (this.createdMs + lingerMs)) + " ms has passed since batch creation plus linger time";
        } else if (this.inRetry() && requestTimeoutMs < (now - (this.lastAttemptMs + retryBackoffMs))) {
            expire = true;
            errorMessage = (now - (this.lastAttemptMs + retryBackoffMs)) + " ms has passed since last attempt plus backoff time";
        }

        if (expire) {
            this.records.close();
            this.done(-1L, Record.NO_TIMESTAMP,
                    new TimeoutException("Expiring " + recordCount + " record(s) for " + topicPartition + " due to " + errorMessage));
        }

        return expire;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    public boolean inRetry() {
        return this.retry;
    }

    /**
     * Set retry to true if the batch is being retried (for send)
     */
    public void setRetry() {
        this.retry = true;
    }
}
