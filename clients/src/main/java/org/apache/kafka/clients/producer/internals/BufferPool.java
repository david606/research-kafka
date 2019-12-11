/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;


/**
 * 只针对特定大小(poolableSize)的ByteBuffer进行管理,其他大小的 ByteBuffer 并不会缓存进 BufferPool<br>
 * 该类相当特定于生产者的需求。在特别是它具有以下属性：
 * <ol>
 * <li>有一个特殊的“可缓冲大小”，此大小的缓冲区保存在空闲列表中并回收
 * <li>它是公平的。那就是所有内存都分配给等待时间最长的线程，直到它有足够的内存为止。
 * 当线程请求大量内存,需要阻塞,直到有多个缓冲区释放，这可以防止饥饿或死锁。
 * </ol>
 */
public final class BufferPool {

    /**
     * 整个 Pool 的大小
     */
    private final long totalMemory;
    private final int poolableSize;
    /**
     * 有多线程并发分配和回收 ByteBuffer ,所以使用锁控制并发
     */
    private final ReentrantLock lock;
    /**
     * 缓存了指定大小的 ByteBuffer 对象
     */
    private final Deque<ByteBuffer> free;
    /**
     * 记录因申请不到足够空间而阻塞的线程 , 此队列中实际记录的是阻塞线程对应的 Condition 对象<br>
     *     添加时将元素的加到队尾addLast,取的时候取第一个元素peekFirst(以确保公平,保证等待时间最长的线程优先获取资源)
     */
    private final Deque<Condition> waiters;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;
    /**
     * 记录了可用的空间大小, 这个空间是 totalMemory 减去 free 列表中全部 ByteBuffer 的大小
     */
    private long availableMemory;

    /**
     * Create a new buffer pool
     *
     * @param memory        The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize  The buffer size to cache in the free list rather than deallocating
     * @param metrics       instance of Metrics
     * @param time          time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<ByteBuffer>();
        this.waiters = new ArrayDeque<Condition>();
        this.totalMemory = memory;
        this.availableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor("bufferpool-wait-time");
        MetricName metricName = metrics.metricName("bufferpool-wait-ratio",
                metricGrpName,
                "The fraction of time an appender waits for space allocation.");
        this.waitTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));
    }

    /**
     * 分配给定大小的缓冲区。如果没有足够的内存，并且缓冲池配置为阻塞模式，则此方法将阻塞
     *
     * @param size             The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException     If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *                                  forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                    + " bytes, but there is a hard limit of "
                    + this.totalMemory
                    + " on memory allocations.");

        this.lock.lock();
        try {
            // 请求的是poolableSize指定大小的ByteBuffer,且free中有空闲的ByteBuffer
            if (size == poolableSize && !this.free.isEmpty())
                return this.free.pollFirst();

            //如果申请的空间不是poolableSize,则执行下面的逻辑

            //计算整个 free 队列的空间(free队列中都是poolableSize大小的ByteBuffer)
            int freeListSize = this.free.size() * this.poolableSize;

            // 现在检查请求是否立即可以满足现有的内存需求，或者我们是否需要阻塞
            if (this.availableMemory + freeListSize >= size) {
                // 为了让 availableMemory>size , freeUp() 方法会从 free 队列中(队尾)不断释ByteBuffer,直到 availableMemory 满足这次申请
                freeUp(size);
                this.availableMemory -= size;//减去要分配出去的size
                lock.unlock();
                // 这里并没有使用 free 队列中的 buffer , 而是直接分配 size 大小的 HeapByteBuffer(用完后不归还free,通过GC回收)
                return ByteBuffer.allocate(size);
            } else {
                //我们内存不足，将不得不阻塞
                int accumulated = 0;
                ByteBuffer buffer = null;

                //将Condition加到waiters队列中
                Condition moreMemory = this.lock.newCondition();
                long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);//Ms毫秒转纳秒Ns,下面的Condition.await 接收Ns单位
                this.waiters.addLast(moreMemory);//Condition放入队列的队尾
                // 循环，直到我们有一个缓冲区或保留足够的内存来分配一次
                while (accumulated < size) {
                    long startWaitNs = time.nanoseconds();
                    long timeNs;
                    boolean waitingTimeElapsed;
                    try {
                        //阻塞等待(直到或超时,或中断,或收到signal通知)
                        //在等待期间,可能会有其它线程释放空间(deallocate()),并从waiters取出第一个元素(Condition),并signal
                        waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        // 异常 , 移除此线程对应的 Condition
                        this.waiters.remove(moreMemory);
                        throw e;
                    } finally {
                        //统计阻塞时间
                        long endWaitNs = time.nanoseconds();
                        timeNs = Math.max(0L, endWaitNs - startWaitNs);
                        this.waitTime.record(timeNs, time.milliseconds());
                    }

                    //等待超时,抛出异常
                    if (waitingTimeElapsed) {
                        this.waiters.remove(moreMemory);
                        throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                    }

                    //计算剩余时间(抠去被阻塞的时间)
                    remainingTimeToBlockNs -= timeNs;

                    // 检查我们是否可以从 free 中满足此要求，否则分配内存
                    if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                        // 只需从空闲列表中获取缓冲区
                        buffer = this.free.pollFirst();
                        accumulated = size;
                    } else {
                        // 我们需要分配内存，但是在此迭代中我们可能只会得到我们所需要的一部分,继续等待
                        freeUp(size - accumulated);//freeUp后的内在给了availableMemory
                        int got = (int) Math.min(size - accumulated, this.availableMemory);
                        this.availableMemory -= got;
                        accumulated += got;
                    }
                }

                // 已经成功分配空间 , 移除该线程的 Condition,让下一个线程开始获取内存
                Condition removed = this.waiters.removeFirst();
                if (removed != moreMemory)
                    throw new IllegalStateException("Wrong condition: this shouldn't happen.");

                // 要是还有空闲空间 , 就唤醒下一个线程
                if (this.availableMemory > 0 || !this.free.isEmpty()) {
                    if (!this.waiters.isEmpty())
                        this.waiters.peekFirst().signal();
                }

                // unlock and return the buffer
                lock.unlock();
                if (buffer == null)
                    return ByteBuffer.allocate(size);
                else
                    return buffer;
            }
        } finally {
            if (lock.isHeldByCurrentThread())
                lock.unlock();
        }
    }

    /**
     * 尝试通过取消分配缓冲池（如果需要）来确保至少有请求的内存字节数用于分配
     *
     * @param size 申请内存大小(字节)
     */
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.availableMemory < size)
            this.availableMemory += this.free.pollLast().capacity();
    }

    /**
     * 将缓冲区返回到池中。如果它们是poolableSize，则将它们添加到空闲free中，否则只需将内存标记为空闲即可(availableMemory += size)。
     *
     * @param buffer 返回的缓冲区
     * @param size   标记为已释放的缓冲区的大小，请注意，此大小可能小于buffer.capacity，因为在就地压缩期间缓冲区可能会重新分配自身
     */
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            //释放的 ByteBuffer 的大小是 poolableSize , 放人 free 队列中进行管理
            if (size == this.poolableSize && size == buffer.capacity()) {
                buffer.clear();
                this.free.add(buffer);
            } else {
                //释放的 ByteBuffer 大小不是 poolableSize , 不会复用 ByteBuffer , 仅修改 availableMemory 的值
                this.availableMemory += size;
            }

            //释放内存后,从waiters队列里,取出等待时间最长的Condition,调用 signal,唤醒因空间不足而阻塞的对应线程
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.availableMemory + this.free.size() * this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.availableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 等待内存阻塞的线程数
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }
}
