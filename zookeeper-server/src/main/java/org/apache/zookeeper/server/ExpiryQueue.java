/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.common.Time;

/**
 * ExpiryQueue tracks elements in time sorted fixed duration buckets.
 * It's used by SessionTrackerImpl to expire sessions and NIOServerCnxnFactory
 * to expire connections.
 * <p>
 * ExpiryQueue跟踪按时间排序的固定持续时间段中的元素。
 * SessionTrackerImpl使用它来使会话和NIOServerCnxnFactory过期
 * 终止连接。
 */
public class ExpiryQueue<E> {

    private final ConcurrentHashMap<E, Long> elemMap = new ConcurrentHashMap<E, Long>();
    /**
     * The maximum number of buckets is equal to max timeout/expirationInterval,
     * so the expirationInterval should not be too small compared to the
     * max timeout that this expiry queue needs to maintain.
     * 存储桶的最大数量等于最大超时/到期间隔，
     * 因此expirationInterval与此到期队列需要维持的最大超时。
     */
    private final ConcurrentHashMap<Long, Set<E>> expiryMap = new ConcurrentHashMap<Long, Set<E>>();

    private final AtomicLong nextExpirationTime = new AtomicLong();
    private final int expirationInterval;

    public ExpiryQueue(int expirationInterval) {
        // 初始化过期时间间隔
        this.expirationInterval = expirationInterval;
        // 计算当前时间所在的会话桶
        nextExpirationTime.set(roundToNextInterval(Time.currentElapsedTime()));
    }

    /**
     * 计算过期时间桶
     * @param time
     * @return
     */
    private long roundToNextInterval(long time) {
        return (time / expirationInterval + 1) * expirationInterval;
    }

    /**
     * Removes element from the queue.
     *
     * @param elem element to remove
     * @return time at which the element was set to expire, or null if
     * it wasn't present
     */
    public Long remove(E elem) {
        Long expiryTime = elemMap.remove(elem);
        if (expiryTime != null) {
            Set<E> set = expiryMap.get(expiryTime);
            if (set != null) {
                set.remove(elem);
                // We don't need to worry about removing empty sets,
                // they'll eventually be removed when they expire.
            }
        }
        return expiryTime;
    }

    /**
     * Adds or updates expiration time for element in queue, rounding the
     * timeout to the expiry interval bucketed used by this queue.
     * 添加或更新队列中元素的到期时间，将超时到此队列使用存储桶的到期时间间隔。
     *
     * @param elem    element to add/update
     * @param timeout timout in milliseconds
     * @return time at which the element is now set to expire if
     * changed, or null if unchanged
     */
    public Long update(E elem/*sessionImpl*/, int timeout) {
        // 获取sessionImpl的上一次所在桶
        Long prevExpiryTime = elemMap.get(elem);
        // 获取当前时间
        long now = Time.currentElapsedTime();
        // 计算session的空闲超时时间点所在的会话桶
        Long newExpiryTime = roundToNextInterval(now + timeout);

        // 上次的会话桶 与当前计算出的会话桶一致，不需要移动会话桶，当前会话也没有超时，故什么都不用做
        if (newExpiryTime.equals(prevExpiryTime)) {
            // No change, so nothing to update
            return null;
        }

        /*
         * 代码走到这说明，上次所在会话桶与现在过期会话桶不一致;因为时间一直向后走，所以 新的桶肯定要大于上次的桶
         * 此时就需要换桶了
         *
         * First add the elem to the new expiry time bucket in expiryMap.
         * 首先将元素添加到expiryMap中的新到期时间段中。
         */

        // 获取当前会话所在的桶
        Set<E> set = expiryMap.get(newExpiryTime);
        if (set == null) {
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(new ConcurrentHashMap<E, Boolean>());
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            Set<E> existingSet = expiryMap.putIfAbsent(newExpiryTime, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        set.add(elem);

        // Map the elem to the new expiry time. If a different previous
        // mapping was present, clean up the previous expiry bucket.
        // 将元素映射到新的到期时间。 存在一个不同的先前映射，请清理先前的到期时段。
        // 将当前会话换桶，之前的桶里有这个session，将之前桶中的会话清理
        prevExpiryTime = elemMap.put(elem, newExpiryTime);
        if (prevExpiryTime != null && !newExpiryTime.equals(prevExpiryTime)) {
            Set<E> prevSet = expiryMap.get(prevExpiryTime);
            if (prevSet != null) {
                prevSet.remove(elem);
            }
        }
        return newExpiryTime;
    }

    /**
     * 直到下一个到期时间的毫秒数；如果已经过去，则返回0
     * @return milliseconds until next expiration time, or 0 if has already past
     */
    public long getWaitTime() {
        long now = Time.currentElapsedTime();
        // 当前时间所在的会话桶
        long expirationTime = nextExpirationTime.get();
        // 如果当前时间还没到 会话桶的时间，返回会话桶时间到当前时间的差值（即当前时间到会话桶时间还剩多少时间）
        // 如果当前时间已经超过会话桶时间，返回0
        return now < expirationTime ? (expirationTime - now) : 0L;
    }

    /**
     * Remove the next expired set of elements from expireMap. This method needs
     * to be called frequently enough by checking getWaitTime(), otherwise there
     * will be a backlog of empty sets queued up in expiryMap.
     *
     * @return next set of expired elements, or an empty set if none are
     * ready
     */
    public Set<E> poll() {
        long now = Time.currentElapsedTime();
        long expirationTime = nextExpirationTime.get();
        if (now < expirationTime) {
            return Collections.emptySet();
        }

        Set<E> set = null;
        long newExpirationTime = expirationTime + expirationInterval;
        if (nextExpirationTime.compareAndSet(expirationTime, newExpirationTime)) {
            set = expiryMap.remove(expirationTime);
        }
        if (set == null) {
            return Collections.emptySet();
        }
        return set;
    }

    public void dump(PrintWriter pwriter) {
        pwriter.print("Sets (");
        pwriter.print(expiryMap.size());
        pwriter.print(")/(");
        pwriter.print(elemMap.size());
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(expiryMap.keySet());
        Collections.sort(keys);
        for (long time : keys) {
            Set<E> set = expiryMap.get(time);
            if (set != null) {
                pwriter.print(set.size());
                pwriter.print(" expire at ");
                pwriter.print(Time.elapsedTimeToDate(time));
                pwriter.println(":");
                for (E elem : set) {
                    pwriter.print("\t");
                    pwriter.println(elem.toString());
                }
            }
        }
    }

    /**
     * Returns an unmodifiable view of the expiration time -&gt; elements mapping.
     */
    public Map<Long, Set<E>> getExpiryMap() {
        return Collections.unmodifiableMap(expiryMap);
    }

}

