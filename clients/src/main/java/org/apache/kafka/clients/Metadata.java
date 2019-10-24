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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 封装一些围绕元数据(metadata)的逻辑的类.
 * <p>
 * 此类由客户端线程（用于分区）和后台发送者线程(Sender)共享。
 * <p>
 * Metadata 只维护部分Topics,可以随时添加．当我们需要一个Topic的metadata而没有时,将会触发metadata的更新
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */
public final class Metadata {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);

    public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;
    private static final long TOPIC_EXPIRY_NEEDS_UPDATE = -1L;

    /**
     * <b>退避 ( backoff ) 时间</b>
     * 两次发岀更新 Cluster 保存的元数据信息的最小时间差 ,默认为100ms
     * 这是为了防止更新操作过于频繁而造成网络阻塞和增加服务端压力
     */
    private final long refreshBackoffMs;
    /**
     * 每隔多久 , 更新一次
     * 默认是 300 x 1000 , 也就是5分种
     */
    private final long metadataExpireMs;

    /**
     * 表示 Kafka 集群元数据的版本号
     * Kafka 集群元数据每更新成功一次 , version 字段的值增 1
     * 通过新旧版本号的比较 , 判断集群元数据是否更新完成
     */
    private int version;

    /**
     * 记录上一次更新元数据的时间戳 ( 也包含更新失败的情况 )
     */
    private long lastRefreshMs;

    /**
     * 上一次成功更新的时间戳
     * 如果每次都成功,则　lastSuccessfulRefreshMs、lastRefreshMs相等。否则lastRefreshMs＞ lastSuccessfulRefreshMs
     */
    private long lastSuccessfulRefreshMs;

    /**
     * 记录 Kafka 集群的元数据
     */
    private Cluster cluster;

    /**
     * 标识是否强制更新 Cluster
     * 这 是 触 发 Sender 线程更新集群元数据的条件之一
     */
    private boolean needUpdate;

    /* ===================Topics with expiry time (到期时间的主题)============================*/

    /**
     * 记录了当前已知的所有 topic,在 cluster 字段中记录了 Topic 最新的元数据
     */
    private final Map<String, Long> topics;

    /**
     * 监听 Metadata 更新的监听器集合
     * 自定义 Metadata 监听实现 Metadata.Listener. onMetadataUpdate() 方法即可 ,
     * 在更新 Metadata 中的 cluster 字段之前 , 会通知 listener 集合中全部 Listener 对象
     */
    private final List<Listener> listeners;


    private final ClusterResourceListeners clusterResourceListeners;

    /**
     * 是否需要更新全部Topic的元数据,
     * 一般情况下,KafkaProducer只维护它用到的Topic的元数据,是集群中全部Topic的子集
     */
    private boolean needMetadataForAllTopics;

    /**
     * 是否开启Topic过期时间
     */
    private final boolean topicExpiryEnabled;

    /**
     * Create a metadata instance with reasonable defaults
     */
    public Metadata() {
        this(100L, 60 * 60 * 1000L);
    }

    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this(refreshBackoffMs, metadataExpireMs, false, new ClusterResourceListeners());
    }

    /**
     * Create a new Metadata instance
     *
     * @param refreshBackoffMs         The minimum amount of time that must expire between metadata refreshes to avoid busy
     *                                 polling
     * @param metadataExpireMs         The maximum amount of time that metadata can be retained without refresh
     * @param topicExpiryEnabled       If true, enable expiry of unused topics
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean topicExpiryEnabled, ClusterResourceListeners clusterResourceListeners) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.topicExpiryEnabled = topicExpiryEnabled;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashMap<>();
        this.listeners = new ArrayList<>();
        this.clusterResourceListeners = clusterResourceListeners;
        this.needMetadataForAllTopics = false;
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * Add the topic to maintain in the metadata. If topic expiry is enabled, expiry time
     * will be reset on the next update.
     */
    public synchronized void add(String topic) {
        topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }

    /**
     * 请求更新当前集群元数据信息，并在更新之前返回当前版本
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true;//设置 needUpdate=true 表示需要强制更新cluster
        return this.version; //返回当前 Kafka 集群元数据的版本号
    }

    /**
     * Check whether an update has been explicitly requested.
     *
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * 等待元数据更新，直到当前版本大于我们所知道的最新版本
     *
     * @param lastVersion 最新版本(更新前)
     * @param maxWaitMs   等待元数据更新的最长时间
     * @throws InterruptedException
     */
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
        if (maxWaitMs < 0) {
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milli seconds");
        }
        long begin = System.currentTimeMillis();
        long remainingWaitMs = maxWaitMs;

        //比较版本号 , 通过版本号比较集群元数据是否更新完成
        while (this.version <= lastVersion) {
            if (remainingWaitMs != 0)
                //主线程(当前线程)等待，直到另一个线程为此对象调用notify（）方法或notifyAll（）方法，或者经过了指定的时间. 当前线程必须拥有该对象的监视器。
                //Sender线程会调用update()方法更新metadata,更新完成后,this.version+=1,然后调用notifyAll(),唤醒所有等待这个对象的monitor的线程
                wait(remainingWaitMs);
            long elapsed = System.currentTimeMillis() - begin;
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            remainingWaitMs = maxWaitMs - elapsed;
        }
    }

    /**
     * Replace the current set of topics maintained to the one provided.
     * If topic expiry is enabled, expiry time of the topics will be
     * reset on the next update.
     *
     * @param topics
     */
    public synchronized void setTopics(Collection<String> topics) {
        if (!this.topics.keySet().containsAll(topics))
            requestUpdate();
        this.topics.clear();
        for (String topic : topics)
            this.topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<>(this.topics.keySet());
    }

    /**
     * Check if a topic is already in the topic set.
     *
     * @param topic topic to check
     * @return true if the topic exists, false otherwise
     */
    public synchronized boolean containsTopic(String topic) {
        return this.topics.containsKey(topic);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     */
    public synchronized void update(Cluster cluster, long now) {
        Objects.requireNonNull(cluster, "cluster should not be null");

        this.needUpdate = false;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.version += 1;

        if (topicExpiryEnabled) {
            // Handle expiry of topics from the metadata refresh set.
            for (Iterator<Map.Entry<String, Long>> it = topics.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Long> entry = it.next();
                long expireMs = entry.getValue();
                if (expireMs == TOPIC_EXPIRY_NEEDS_UPDATE)
                    entry.setValue(now + TOPIC_EXPIRY_MS);
                else if (expireMs <= now) {
                    it.remove();
                    log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", entry.getKey(), expireMs, now);
                }
            }
        }

        for (Listener listener : listeners)
            listener.onMetadataUpdate(cluster);

        String previousClusterId = cluster.clusterResource().clusterId();

        if (this.needMetadataForAllTopics) {
            // the listener may change the interested topics, which could cause another metadata refresh.
            // If we have already fetched all topics, however, another fetch should be unnecessary.
            this.needUpdate = false;
            this.cluster = getClusterForCurrentTopics(cluster);
        } else {
            this.cluster = cluster;
        }

        // The bootstrap cluster is guaranteed not to have any useful information
        if (!cluster.isBootstrapConfigured()) {
            String clusterId = cluster.clusterResource().clusterId();
            if (clusterId == null ? previousClusterId != null : !clusterId.equals(previousClusterId))
                log.info("Cluster ID: {}", cluster.clusterResource().clusterId());
            clusterResourceListeners.onUpdate(cluster.clusterResource());
        }

        notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
        this.lastRefreshMs = now;
    }

    /**
     * @return The current metadata version
     */
    public synchronized int version() {
        return this.version;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * The metadata refresh backoff in ms
     */
    public long refreshBackoff() {
        return refreshBackoffMs;
    }

    /**
     * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
     *
     * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
     */
    public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
        this.needMetadataForAllTopics = needMetadataForAllTopics;
    }

    /**
     * Get whether metadata for all topics is needed or not
     */
    public synchronized boolean needMetadataForAllTopics() {
        return this.needMetadataForAllTopics;
    }

    /**
     * Add a Metadata listener that gets notified of metadata updates
     */
    public synchronized void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Stop notifying the listener of metadata updates
     */
    public synchronized void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    /**
     * MetadataUpdate Listener
     */
    public interface Listener {
        void onMetadataUpdate(Cluster cluster);
    }

    private Cluster getClusterForCurrentTopics(Cluster cluster) {
        Set<String> unauthorizedTopics = new HashSet<>();
        Collection<PartitionInfo> partitionInfos = new ArrayList<>();
        List<Node> nodes = Collections.emptyList();
        Set<String> internalTopics = Collections.emptySet();
        String clusterId = null;
        if (cluster != null) {
            clusterId = cluster.clusterResource().clusterId();
            internalTopics = cluster.internalTopics();
            unauthorizedTopics.addAll(cluster.unauthorizedTopics());
            unauthorizedTopics.retainAll(this.topics.keySet());

            for (String topic : this.topics.keySet()) {
                List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
                if (partitionInfoList != null) {
                    partitionInfos.addAll(partitionInfoList);
                }
            }
            nodes = cluster.nodes();
        }
        return new Cluster(clusterId, nodes, partitionInfos, unauthorizedTopics, internalTopics);
    }
}
