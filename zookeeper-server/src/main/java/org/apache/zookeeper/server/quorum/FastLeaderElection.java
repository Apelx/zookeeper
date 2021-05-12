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

package org.apache.zookeeper.server.quorum;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumOracleMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 * <p>
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */

public class FastLeaderElection implements Election {

    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    static final int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    private static int maxNotificationInterval = 60000;

    /**
     * Lower bound for notification check. The observer don't need to use
     * the same lower bound as participant members
     */
    private static int minNotificationInterval = finalizeWait;

    /**
     * Minimum notification interval, default is equal to finalizeWait
     */
    public static final String MIN_NOTIFICATION_INTERVAL = "zookeeper.fastleader.minNotificationInterval";

    /**
     * Maximum notification interval, default is 60s
     */
    public static final String MAX_NOTIFICATION_INTERVAL = "zookeeper.fastleader.maxNotificationInterval";

    static {
        minNotificationInterval = Integer.getInteger(MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        LOG.info("{} = {} ms", MIN_NOTIFICATION_INTERVAL, minNotificationInterval);
        maxNotificationInterval = Integer.getInteger(MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
        LOG.info("{} = {} ms", MAX_NOTIFICATION_INTERVAL, maxNotificationInterval);
    }

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    QuorumCnxManager manager;

    private SyncedLearnerTracker leadingVoteSet;

    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */

    public static class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public static final int CURRENTVERSION = 0x2;
        int version;

        /*
         * Proposed leader
         */ long leader;

        /*
         * zxid of the proposed leader
         */ long zxid;

        /*
         * Epoch
         */ long electionEpoch;

        /*
         * current state of sender
         */ QuorumPeer.ServerState state;

        /*
         * Address of sender
         */ long sid;

        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         */ long peerEpoch;

    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    public static class ToSend {

        enum mType {
            crequest,
            challenge,
            notification,
            ack
        }

        ToSend(mType type, long leader, long zxid, long electionEpoch, ServerState state, long sid, long peerEpoch, byte[] configData) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
            this.configData = configData;
        }

        /*
         * Proposed leader in the case of notification
         * 通知中提案推荐的leader
         */ long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         * 通知推荐者的zxid
         */ long zxid;

        /*
         * Epoch
         * 通知推荐者的epoch
         */ long electionEpoch;

        /*
         * Current state;
         * 通知推荐者的当前状态
         */ QuorumPeer.ServerState state;

        /*
         * Address of recipient
         * 通知推荐者的server id
         */ long sid;

        /*
         * Used to send a QuorumVerifier (configuration info)
         */ byte[] configData = dummyData;

        /*
         * Leader epoch
         */ long peerEpoch;

    }

    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */

    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */

        class WorkerReceiver extends ZooKeeperThread {

            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if (response == null) {
                            continue;
                        }

                        final int capacity = response.buffer.capacity();

                        // The current protocol and two previous generations all send at least 28 bytes
                        if (capacity < 28) {
                            LOG.error("Got a short response from server {}: {}", response.sid, capacity);
                            continue;
                        }

                        // this is the backwardCompatibility mode in place before ZK-107
                        // It is for a version of the protocol in which we didn't send peer epoch
                        // With peer epoch and version the message became 40 bytes
                        boolean backCompatibility28 = (capacity == 28);

                        // this is the backwardCompatibility mode for no version information
                        boolean backCompatibility40 = (capacity == 40);

                        response.buffer.clear();

                        // Instantiate Notification and set its attributes
                        Notification n = new Notification();

                        int rstate = response.buffer.getInt();
                        long rleader = response.buffer.getLong();
                        long rzxid = response.buffer.getLong();
                        long relectionEpoch = response.buffer.getLong();
                        long rpeerepoch;

                        int version = 0x0;
                        QuorumVerifier rqv = null;

                        try {
                            if (!backCompatibility28) {
                                rpeerepoch = response.buffer.getLong();
                                if (!backCompatibility40) {
                                    /*
                                     * Version added in 3.4.6
                                     */

                                    version = response.buffer.getInt();
                                } else {
                                    LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                                }
                            } else {
                                LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                                rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                            }

                            // check if we have a version that includes config. If so extract config info from message.
                            if (version > 0x1) {
                                int configLength = response.buffer.getInt();

                                // we want to avoid errors caused by the allocation of a byte array with negative length
                                // (causing NegativeArraySizeException) or huge length (causing e.g. OutOfMemoryError)
                                if (configLength < 0 || configLength > capacity) {
                                    throw new IOException(String.format("Invalid configLength in notification message! sid=%d, capacity=%d, version=%d, configLength=%d",
                                            response.sid, capacity, version, configLength));
                                }

                                byte[] b = new byte[configLength];
                                response.buffer.get(b);

                                synchronized (self) {
                                    try {
                                        rqv = self.configFromString(new String(b, UTF_8));
                                        QuorumVerifier curQV = self.getQuorumVerifier();
                                        if (rqv.getVersion() > curQV.getVersion()) {
                                            LOG.info("{} Received version: {} my version: {}",
                                                    self.getId(),
                                                    Long.toHexString(rqv.getVersion()),
                                                    Long.toHexString(self.getQuorumVerifier().getVersion()));
                                            if (self.getPeerState() == ServerState.LOOKING) {
                                                LOG.debug("Invoking processReconfig(), state: {}", self.getServerState());
                                                self.processReconfig(rqv, null, null, false);
                                                if (!rqv.equals(curQV)) {
                                                    LOG.info("restarting leader election");
                                                    self.shuttingDownLE = true;
                                                    self.getElectionAlg().shutdown();

                                                    break;
                                                }
                                            } else {
                                                LOG.debug("Skip processReconfig(), state: {}", self.getServerState());
                                            }
                                        }
                                    } catch (IOException | ConfigException e) {
                                        LOG.error("Something went wrong while processing config received from {}", response.sid);
                                    }
                                }
                            } else {
                                LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                            }
                        } catch (BufferUnderflowException | IOException e) {
                            LOG.warn("Skipping the processing of a partial / malformed response message sent by sid={} (message length: {})",
                                    response.sid, capacity, e);
                            continue;
                        }
                        /*
                         * If it is from a non-voting server (such as an observer or
                         * a non-voting follower), respond right away.
                         */
                        if (!validVoter(response.sid)) {
                            Vote current = self.getCurrentVote();
                            QuorumVerifier qv = self.getQuorumVerifier();
                            ToSend notmsg = new ToSend(
                                    ToSend.mType.notification,
                                    current.getId(),
                                    current.getZxid(),
                                    logicalclock.get(),
                                    self.getPeerState(),
                                    response.sid,
                                    current.getPeerEpoch(),
                                    qv.toString().getBytes(UTF_8));

                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message
                            LOG.debug("Receive new notification message. My id = {}", self.getId());

                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (rstate) {
                                case 0:
                                    ackstate = QuorumPeer.ServerState.LOOKING;
                                    break;
                                case 1:
                                    ackstate = QuorumPeer.ServerState.FOLLOWING;
                                    break;
                                case 2:
                                    ackstate = QuorumPeer.ServerState.LEADING;
                                    break;
                                case 3:
                                    ackstate = QuorumPeer.ServerState.OBSERVING;
                                    break;
                                default:
                                    continue;
                            }

                            n.leader = rleader;
                            n.zxid = rzxid;
                            n.electionEpoch = relectionEpoch;
                            n.state = ackstate;
                            n.sid = response.sid;
                            n.peerEpoch = rpeerepoch;
                            n.version = version;
                            n.qv = rqv;
                            /*
                             * Print notification info
                             */
                            LOG.info(
                                    "Notification: my state:{}; n.sid:{}, n.state:{}, n.leader:{}, n.round:0x{}, "
                                            + "n.peerEpoch:0x{}, n.zxid:0x{}, message format version:0x{}, n.config version:0x{}",
                                    self.getPeerState(),
                                    n.sid,
                                    n.state,
                                    n.leader,
                                    Long.toHexString(n.electionEpoch),
                                    Long.toHexString(n.peerEpoch),
                                    Long.toHexString(n.zxid),
                                    Long.toHexString(n.version),
                                    (n.qv != null ? (Long.toHexString(n.qv.getVersion())) : "0"));

                            /*
                             * If this server is looking, then send proposed leader
                             */

                            if (self.getPeerState() == QuorumPeer.ServerState.LOOKING) {
                                recvqueue.offer(n);

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if ((ackstate == QuorumPeer.ServerState.LOOKING)
                                        && (n.electionEpoch < logicalclock.get())) {
                                    Vote v = getVote();
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(
                                            ToSend.mType.notification,
                                            v.getId(),
                                            v.getZxid(),
                                            logicalclock.get(),
                                            self.getPeerState(),
                                            response.sid,
                                            v.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                Vote current = self.getCurrentVote();
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    if (self.leader != null) {
                                        if (leadingVoteSet != null) {
                                            self.leader.setLeadingVoteSet(leadingVoteSet);
                                            leadingVoteSet = null;
                                        }
                                        self.leader.reportLookingSid(response.sid);
                                    }


                                    LOG.debug(
                                            "Sending new notification. My id ={} recipient={} zxid=0x{} leader={} config version = {}",
                                            self.getId(),
                                            response.sid,
                                            Long.toHexString(current.getZxid()),
                                            current.getId(),
                                            Long.toHexString(self.getQuorumVerifier().getVersion()));

                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(
                                            ToSend.mType.notification,
                                            current.getId(),
                                            current.getZxid(),
                                            current.getElectionEpoch(),
                                            self.getPeerState(),
                                            response.sid,
                                            current.getPeerEpoch(),
                                            qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message", e);
                    }
                }
                LOG.info("WorkerReceiver is down");
            }

        }

        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */

        class WorkerSender extends ZooKeeperThread {

            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while (!stop) {
                    try {
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if (m == null) {
                            continue;
                        }

                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m message to send
             */
            void process(ToSend m) {
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(), m.leader, m.zxid, m.electionEpoch, m.peerEpoch, m.configData);

                manager.toSend(m.sid, requestBuffer);

            }

        }

        WorkerSender ws;
        WorkerReceiver wr;
        Thread wsThread = null;
        Thread wrThread = null;

        /**
         * Constructor of class Messenger.
         *
         * @param manager Connection manager
         */
        Messenger(QuorumCnxManager manager) {

            this.ws = new WorkerSender(manager);

            this.wsThread = new Thread(this.ws, "WorkerSender[myid=" + self.getId() + "]");
            this.wsThread.setDaemon(true);

            this.wr = new WorkerReceiver(manager);

            this.wrThread = new Thread(this.wr, "WorkerReceiver[myid=" + self.getId() + "]");
            this.wrThread.setDaemon(true);
        }

        /**
         * Starts instances of WorkerSender and WorkerReceiver
         */
        void start() {
            this.wsThread.start();
            this.wrThread.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */
    long proposedLeader;
    long proposedZxid;
    long proposedEpoch;

    /**
     * Returns the current value of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock.get();
    }

    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch) {
        byte[] requestBytes = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch, byte[] configData) {
        byte[] requestBytes = new byte[44 + configData.length];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);
        requestBuffer.putInt(configData.length);
        requestBuffer.put(configData);

        return requestBuffer;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        this.manager = manager;
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        sendqueue = new LinkedBlockingQueue<ToSend>();
        recvqueue = new LinkedBlockingQueue<Notification>();
        this.messenger = new Messenger(manager);
    }

    /**
     * This method starts the sender and receiver threads.
     */
    public void start() {
        this.messenger.start();
    }

    private void leaveInstance(Vote v) {
        LOG.debug(
                "About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}",
                v.getId(),
                Long.toHexString(v.getZxid()),
                self.getId(),
                self.getPeerState());
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;

    public void shutdown() {
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        leadingVoteSet = null;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * Send notifications to all peers upon a change in our vote
     */
    private void sendNotifications() {
        for (long sid : self.getCurrentAndNextConfigVoters()) {
            QuorumVerifier qv = self.getQuorumVerifier();
            ToSend notmsg = new ToSend(
                    ToSend.mType.notification,
                    proposedLeader,
                    proposedZxid,
                    logicalclock.get(),
                    QuorumPeer.ServerState.LOOKING,
                    sid,
                    proposedEpoch,
                    qv.toString().getBytes(UTF_8));

            LOG.debug(
                    "Sending Notification: {} (n.leader), 0x{} (n.zxid), 0x{} (n.round), {} (recipient),"
                            + " {} (myid), 0x{} (n.peerEpoch) ",
                    proposedLeader,
                    Long.toHexString(proposedZxid),
                    Long.toHexString(logicalclock.get()),
                    sid,
                    self.getId(),
                    Long.toHexString(proposedEpoch));

            sendqueue.offer(notmsg);
        }
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug(
                "id: {}, proposed id: {}, zxid: 0x{}, proposed zxid: 0x{}",
                newId,
                curId,
                Long.toHexString(newZxid),
                Long.toHexString(curZxid));
        // 权重为0，即当前是observer
        if (self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }

        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */
        // 先比较epoch，大的当选
        // epoch相同再比较 zxid(epoch实际与zxid是同一个long，只是epoch是高32位, zxid是低32位)，zxid大的当选
        // zxid相同再比较serverId(myId), serverId大的当选
        return ((newEpoch > curEpoch)
                || ((newEpoch == curEpoch)
                && ((newZxid > curZxid)
                || ((newZxid == curZxid)
                && (newId > curId)))));
    }

    /**
     * Given a set of votes, return the SyncedLearnerTracker which is used to
     * determines if have sufficient to declare the end of the election round.
     * 给定一组投票，返回SyncedLearnerTracker, 确定是否足以宣布选举回合结束
     * @param votes Set of votes
     * @param vote  Identifier of the vote received last
     * @return the SyncedLearnerTracker with vote details
     */
    protected SyncedLearnerTracker getVoteTracker(Map<Long, Vote> votes, Vote vote) {
        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();
        voteSet.addQuorumVerifier(self.getQuorumVerifier());
        if (self.getLastSeenQuorumVerifier() != null
                && self.getLastSeenQuorumVerifier().getVersion() > self.getQuorumVerifier().getVersion()) {
            voteSet.addQuorumVerifier(self.getLastSeenQuorumVerifier());
        }

        /*
         * First make the views consistent. Sometimes peers will have different
         * zxids for a server depending on timing.
         */
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                voteSet.addAck(entry.getKey());
            }
        }

        return voteSet;
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes         set of votes
     * @param leader        leader id
     * @param electionEpoch epoch id
     */
    protected boolean checkLeader(Map<Long, Vote> votes, long leader, long electionEpoch) {

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */
        // 如果leader不是当前server
        if (leader != self.getId()) {
            // 投票中leader为null则不承认leader
            if (votes.get(leader) == null) {
                predicate = false;
            }
            // 投票中leader状态不是leading,不承认leader
            else if (votes.get(leader).getState() != ServerState.LEADING) {
                predicate = false;
            }
        }
        // 当前server的逻辑时钟与leader的逻辑时钟不一致，不承认leader
        else if (logicalclock.get() != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    synchronized void updateProposal(long leader, long zxid, long epoch) {
        LOG.debug(
                "Updating proposal: {} (newleader), 0x{} (newzxid), {} (oldleader), 0x{} (oldzxid)",
                leader,
                Long.toHexString(zxid),
                proposedLeader,
                Long.toHexString(proposedZxid));

        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    public synchronized Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I am a participant: {}", self.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I am an observer: {}", self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId() {
        if (self.getQuorumVerifier().getVotingMembers().containsKey(self.getId())) {
            return self.getId();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            return self.getLastLoggedZxid();
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            try {
                return self.getCurrentEpoch();
            } catch (IOException e) {
                RuntimeException re = new RuntimeException(e.getMessage());
                re.setStackTrace(e.getStackTrace());
                throw re;
            }
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Update the peer state based on the given proposedLeader. Also update
     * the leadingVoteSet if it becomes the leader.
     */
    private void setPeerState(long proposedLeader, SyncedLearnerTracker voteSet) {
        ServerState ss = (proposedLeader == self.getId()) ? ServerState.LEADING : learningState();
        self.setPeerState(ss);
        if (ss == ServerState.LEADING) {
            leadingVoteSet = voteSet;
        }
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }

        self.start_fle = Time.currentElapsedTime();
        try {
            /*
             * The votes from the current leader election are stored in recvset. In other words, a vote v is in recvset
             * if v.electionEpoch == logicalclock. The current participant uses recvset to deduce on whether a majority
             * of participants has voted for it.
             */
            /*
             * 当前Leader选举的选票存储在recvset中。 换句话说，如果v.electionEpoch ==逻辑时钟，
             * 则表决v处于recvset中（同一轮选举）。 当前参与者使用recvset来推断是否有大多数参与者投票支持它。
             * 相当于票箱
             */
            Map<Long, Vote> recvset = new HashMap<Long, Vote>();

            /*
             * The votes from previous leader elections, as well as the votes from the current leader election are
             * stored in outofelection. Note that notifications in a LOOKING state are not stored in outofelection.
             * Only FOLLOWING or LEADING notifications are stored in outofelection. The current participant could use
             * outofelection to learn which participant is the leader if it arrives late (i.e., higher logicalclock than
             * the electionEpoch of the received notifications) in a leader election.
             */
            /*
             * 先前的领导人选举的票数以及当前的领导人选举的票数都存储在大选中。
             * 请注意，处于LOOKING状态的通知不会存储在outofelection中。
             * 只有FOLLOWING或LEADING通知以超选形式存储。
             * 如果当前参加者在领导者选举中迟到（即比接收到的通知的lectionEpoch更高的逻辑时钟）到达，
             * 则可以使用选民来了解哪个参与者是领导者。
             * 即存放非Locking状态发来的通知
             */
            Map<Long, Vote> outofelection = new HashMap<Long, Vote>();

            // notification time out 通知超时时间
            int notTimeout = minNotificationInterval;

            synchronized (this) {
                // 逻辑时钟（选举期间的epoch+1）
                logicalclock.incrementAndGet();
                // 更新当前server的提案信息, 即我选我
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            LOG.info(
                    "New election. My id = {}, proposed zxid=0x{}",
                    self.getId(),
                    Long.toHexString(proposedZxid));
            // 将当前server的提案信息放到队列的列尾, 广播给其他法定server
            sendNotifications();

            // 同步learner(follower、observer)追踪器
            SyncedLearnerTracker voteSet = null;

            /*
             * Loop in which we exchange notifications until we find a leader
             * 一直循环至leader被找到
             */

            while ((self.getPeerState() == ServerState.LOOKING) && (!stop)) {
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 * 从队列中删除下一个通知，在终止时间的2倍后超时
                 * 即从通知队列头部, 不断循环取出通知
                 */
                Notification n = recvqueue.poll(notTimeout, TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 * 如果未收到足够的通知，则发送更多通知。 否则，将处理新的通知。
                 */
                // n=null 表示此时我收到的通知读取完了
                if (n == null) {
                    // 判断当前server与集群中其他server是否失联
                    if (manager.haveDelivered()) {
                        // 还未失联，则再次将当前server的提案推荐信息放到队列中，广播出去
                        sendNotifications();
                    } else {
                        // 若当前server已经失联，仅仅做重新连接操作
                        // 这就是设计精妙之处：若当前server与其他server失联了，在其他server还未选举出leader之前，如果未收齐所有server的响应，
                        // 会一直向其他server发通知，而当前server重连上之后只需等待其他server给我发通知就可以
                        manager.connectAll();
                    }

                    /*
                     * Exponential backoff
                     * 指数退避
                     */
                    // 超时时间为上次超时时间*2(刚开始是200毫秒)
                    int tmpTimeOut = notTimeout * 2;
                    // 如果超时时间超过最大60秒，那么一直为60秒
                    notTimeout = Math.min(tmpTimeOut, maxNotificationInterval);

                    /*
                     * When a leader failure happens on a master, the backup will be supposed to receive the honour from
                     * Oracle and become a leader, but the honour is likely to be delay. We do a re-check once timeout happens
                     * 如果在主服务器上发生领导者故障，则备份应该会从Oracle获得荣誉并成为领导者，但是该荣誉很可能会延迟。
                     *  一旦发生超时，我们会重新检查
                     *
                     * The leader election algorithm does not provide the ability of electing a leader from a single instance
                     * which is in a configuration of 2 instances.
                     * 领导者选举算法不提供从单个实例中选举领导者的能力
                     * 这是2个实例的配置。
                     * */
                    self.getQuorumVerifier().revalidateVoteset(voteSet, notTimeout != minNotificationInterval);
                    // 校验当前sever是不是投票过半了，并且还未超时
                    if (self.getQuorumVerifier() instanceof QuorumOracleMaj && voteSet != null && voteSet.hasAllQuorums() && notTimeout != minNotificationInterval) {
                        // 修改状态，结束选举
                        setPeerState(proposedLeader, voteSet);
                        Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch);
                        leaveInstance(endVote);
                        return endVote;
                    }

                    LOG.info("Notification time out: {} ms", notTimeout);

                }
                // 取出队列中的每一个通知，校验通知中投票人的身份，与推荐leader的身份，校验成功进入下面
                else if (validVoter(n.sid) && validVoter(n.leader)) {
                    /*
                     * Only proceed if the vote comes from a replica in the current or next
                     * voting view for a replica in the current or next voting view.
                     * 仅当投票来自当前或下一个副本时才继续进行, 当前或下一个投票视图中副本的投票视图。
                     */
                    // switch 通知中投票人的状态
                    switch (n.state) {
                        case LOOKING:
                            if (getInitLastLoggedZxid() == -1) {
                                LOG.debug("Ignoring notification as our zxid is -1");
                                break;
                            }
                            if (n.zxid == -1) {
                                LOG.debug("Ignoring notification from member with -1 zxid {}", n.sid);
                                break;
                            }
                            // 如果通知中投票人的逻辑时钟比当前server大(可能由于网络原因)，当前server的这一轮选举应该不作数
                            // If notification > current, replace and send messages out
                            if (n.electionEpoch > logicalclock.get()) {
                                // 修改当前server的逻辑时钟与通知中投票人的逻辑时钟一致
                                logicalclock.set(n.electionEpoch);
                                // 清空当前server的票箱
                                recvset.clear();
                                // 校验通知中的推荐Leader是不是比当前server推荐的leader更适合，更适合返回true,否则返回false
                                // 比较顺序(epoch -> zxid -> serverId(my id))
                                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                    updateProposal(n.leader, n.zxid, n.peerEpoch);
                                } else {
                                    updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
                                }
                                // 不管谁更合适，都会更新当前sever推荐leader信息,并且广播出去
                                sendNotifications();
                            }
                            // 如果通知中投票人的逻辑时钟比当前server小(可能由于网络原因)，通知n所在选举那一轮应该不作数
                            else if (n.electionEpoch < logicalclock.get()) {
                                // 记录一下日志，不予理会
                                LOG.debug(
                                        "Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x{}, logicalclock=0x{}",
                                        Long.toHexString(n.electionEpoch),
                                        Long.toHexString(logicalclock.get()));
                                break;
                            }
                            // 通知n中投票人的逻辑时钟与当前server一致，说明通知n与当前server在同一轮选举中
                            // 比较通知中的推荐Leader是不是比当前server推荐的leader更适合，更适合返回true,否则返回false
                            else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                // 如果n比当前server推荐的更核时，更新当前server的推荐信息并广播出去
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                                sendNotifications();
                            }

                            LOG.debug(
                                    "Adding vote: from={}, proposed leader={}, proposed zxid=0x{}, proposed election epoch=0x{}",
                                    n.sid,
                                    n.leader,
                                    Long.toHexString(n.zxid),
                                    Long.toHexString(n.electionEpoch));

                            // don't care about the version if it's in LOOKING state
                            // 比较完谁更合适并广播之后，将通知n的投票信息放到票箱中
                            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));

                            // 给定一组投票，返回SyncedLearnerTracker, 确定是否足以宣布选举回合结束
                            voteSet = getVoteTracker(recvset, new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch));

                            // 判断当前server推荐的leader是否投票过半
                            if (voteSet.hasAllQuorums()) {

                                // Verify if there is any change in the proposed leader
                                // 判断剩余选票中有没有更适合做leader的
                                // 注意，某选票支持率过半仅仅是最低要求
                                // 当前while()有两个出口：
                                // 1)while()循环条件：若从该出口跳出循环，则说明剩余的通知中没有比当前我们推荐
                                //    的leader更适合做leader的了
                                // 2)break：若从该出口跳出循环，则说明剩余的通知中发现了比当前我们推荐的leader
                                //    更适合做leader的了
                                while ((n = recvqueue.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                                    // 比较通知中的推荐Leader是不是比当前server推荐的leader更适合，更适合返回true,否则返回false
                                    if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                        // 将通知n放回到队列, 重新走前面的流程
                                        recvqueue.put(n);
                                        break;
                                    }
                                }

                                /*
                                 * This predicate is true once we don't read any new
                                 * relevant message from the reception queue
                                 */
                                // 若n为null，说明前面的while()循环是从第一个出口出去的，则说明
                                // 我们推荐的leader就是真正的leader了，然后就可以做收尾工作了
                                if (n == null) {
                                    // 修改状态, 结束选举
                                    setPeerState(proposedLeader, voteSet);
                                    Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch);
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }
                            break;
                        // 代码若可以匹配上observing，则说明当前通知是observer发送的。
                        // 而observer发出的通知是无法通过前面的第1044行的验证的。
                        // 这里出现了矛盾
                        case OBSERVING:
                            LOG.debug("Notification from observer: {}", n.sid);
                            break;

                        /*
                         * In ZOOKEEPER-3922, we separate the behaviors of FOLLOWING and LEADING.
                         * To avoid the duplication of codes, we create a method called followingBehavior which was used to
                         * shared by FOLLOWING and LEADING. This method returns a Vote. When the returned Vote is null, it follows
                         * the original idea to break swtich statement; otherwise, a valid returned Vote indicates, a leader
                         * is generated.
                         *
                         * 在ZOOKEEPER-3922中，我们将FOLLOWING和LEADING的行为分开。为了避免重复代码，我们创建了一个名为followingBehavior的方法，
                         * 该方法已被FOLLOWING和LEADING共享。此方法返回一个投票。当返回的Vote为null时，它遵循最初的想法来中断swtich语句；
                         * 否则，返回的有效投票将指示生成领导者。
                         *
                         * The reason why we need to separate these behaviors is to make the algorithm runnable for 2-node
                         * setting. An extra condition for generating leader is needed. Due to the majority rule, only when
                         * there is a majority in the voteset, a leader will be generated. However, in a configuration of 2 nodes,
                         * the number to achieve the majority remains 2, which means a recovered node cannot generate a leader which is
                         * the existed leader. Therefore, we need the Oracle to kick in this situation. In a two-node configuration, the Oracle
                         * only grants the permission to maintain the progress to one node. The oracle either grants the permission to the
                         * remained node and makes it a new leader when there is a faulty machine, which is the case to maintain the progress.
                         * Otherwise, the oracle does not grant the permission to the remained node, which further causes a service down.
                         *
                         * 我们需要分开这些行为的原因是使该算法可用于2节点设置。需要产生领导者的额外条件。由于大多数规则，只有在选票中有大多数时，
                         * 将产生领导者。但是，在2个节点的配置中，实现多数的数目仍然为2，这意味着恢复的节点无法生成前导节点，即
                         * 现有的领导者。因此，我们需要Oracle在这种情况下发挥作用。在两节点配置中，Oracle仅向一个节点授予维护进度的权限。
                         *  oracle会向剩余的节点授予权限，并在出现故障的计算机时使其成为新的领导者，以保持进度。
                         * 否则，oracle不会将权限授予剩余的节点，这进一步导致服务中断。
                         *
                         *
                         * In the former case, when a failed server recovers and participate in the leader election, it would not locate a
                         * new leader because there does not exist a majority in the voteset. It fails on the containAllQuorum() infinitely due to
                         * two facts. First one is the fact that it does do not have a majority in the voteset. The other fact is the fact that
                         * the oracle would not give the permission since the oracle already gave the permission to the existed leader, the healthy machine.
                         * Logically, when the oracle replies with negative, it implies the existed leader which is LEADING notification comes from is a valid leader.
                         * To threat this negative replies as a permission to generate the leader is the purpose to separate these two behaviors.
                         *
                         * 在前一种情况下，当发生故障的服务器恢复正常并参加领导者选举时，它不会找到新的领导者，因为在投票集中没有多数席位。
                         *  由于两个事实，它在containAllQuorum（）上无限期失败。 第一个事实是，它在选票中没有多数席位。
                         *  另一个事实是甲骨文不会给予许可，因为甲骨文已经将许可权授予了存在的领导者健康的机器。
                         * 逻辑上，当甲骨文答复否定时，这意味着存在的领导者LEADING通知来自 是有效的领导者。
                         * 威胁要否定这种否定答复，以作为产生领导者的权限，目的是要区分这两种行为。
                         *
                         * */
                        // 首先要清楚两点：
                        // 1)无论当前server处于什么状态，只要接收到外部server发来的通知，当前server
                        //    就会向那个外部server发送自己的通知
                        // 2)一个server只要其能接收到其它server的通知，就说明这个server不是Observer

                        // 什么场景下我们会收到来自于follower或leader的通知呢？
                        // 场景一：若有新的server要加入到一个正常工作的集群时，该Server在启动时其状态就
                        //    是looking，要查找leader，向外发出通知。此时的leader与follower在接收到它的
                        //     通知后就会向其回复。那么该server接收到的通知就是following与leading状态的
                        // 场景二：当其它server已经在本轮选举中选举出了新的leader，但还没有通知到当前server，
                        //     所以当前server的状态仍是looking，但其接收到的其它通知状态就有可能是leading
                        //     或following
                        // 通知n的投票人是following状态(投票的server已经是follower了)
                        case FOLLOWING:
                            /*
                             * To avoid duplicate codes
                             * 为了避免重复代码
                             * */
                            /*
                             * 判断follower通知是不是与当前server在同一轮选举中，如果是的话，判断推荐的leader在当前server的票箱中投票是否过半并且推荐的leader状态没有问题
                             * 都满足，则选举结束，当前server找到了leader
                             * 如果follower通知不是与当前server在同一轮选举总，或者follower推荐的leader在当前server这投票不过半，又或者推荐的leader挂了，
                             * 就会将follower推荐的通知n放到非locking状态投票的票箱中
                             * 然后判断follower推荐的leader是不是在follower中的投票过半，并且没有问题，没有问题的话，则找到了leader,结束选举
                             * 如果还是有问题，则结束switch，继续循环
                             */
                            Vote resultFN = receivedFollowingNotification(recvset, outofelection, voteSet, n);
                            if (resultFN == null) {
                                break;
                            } else {
                                return resultFN;
                            }
                        // 通知n的投票人是leading状态(投票的server已经是leader了)
                        case LEADING:
                            /*
                             * In leadingBehavior(), it performs followingBehvior() first. When followingBehavior() returns
                             * a null pointer, ask Oracle whether to follow this leader.
                             * */
                            // 调用receivedFollowingNotification，还不能确定leader，再判断 当前server是否可以跟从该领导者(是否是observer)
                            // 如果是observer则直接认定 n推荐的leader做leader,结束选举，否则退出switch继续循环
                            Vote resultLN = receivedLeadingNotification(recvset, outofelection, voteSet, n);
                            if (resultLN == null) {
                                break;
                            } else {
                                return resultLN;
                            }
                        default:
                            LOG.warn("Notification state unrecognized: {} (n.state), {}(n.sid)", n.state, n.sid);
                            break;
                    }
                } else {
                    if (!validVoter(n.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if (self.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}", manager.getConnectionThreadCount());
        }
    }

    private Vote receivedFollowingNotification(Map<Long, Vote> recvset, Map<Long, Vote> outofelection, SyncedLearnerTracker voteSet, Notification n) {
        /*
         * Consider all notifications from the same epoch
         * together.
         */
        // 通知n所在选举与当前server是同一轮
        if (n.electionEpoch == logicalclock.get()) {
            // 将通知n(following)的投票放到当前server的票箱中
            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
            voteSet = getVoteTracker(recvset, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
            // 判断当前server是否应该退出选举了
            // 若要我承认你们推荐的这个leader，需要满足两个条件：
            // 1)follower推荐的这个leader在我的票箱中支持率过半
            // 2)follower推荐的这个leader的主机状态没有问题(校验leader与当前server的逻辑时钟)
            if (voteSet.hasAllQuorums() && checkLeader(recvset, n.leader, n.electionEpoch)) {
                // 结束选举修改状态
                setPeerState(n.leader, voteSet);
                Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                leaveInstance(endVote);
                return endVote;
            }
        }

        // 走到这说明要么通知n所在选举与当前server不是同一轮，要么就是follower推荐的leader在我的票箱里投票不过半，要么follower推荐的leader状态有问题
        /*
         * Before joining an established ensemble, verify that
         * a majority are following the same leader.
         * 加入已建立的合奏之前，请验证大多数人都跟随同一个领导者。
         *
         * Note that the outofelection map also stores votes from the current leader election.
         * See ZOOKEEPER-1732 for more information.
         * 请注意，大选后的地图还会存储当前领导人选举的选票。有关更多信息，请参见ZOOKEEPER-1732。
         */
        // 向非法票箱（非locking状态发来的投票）里
        outofelection.put(n.sid, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
        voteSet = getVoteTracker(outofelection, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
        // 处理场景一的情况：
        // 若让我承认你们所告诉我的这个leader，必须要满足两个条件:
        // 1)你们推荐的leader在你们的通知中支持率要过半
        // 2)你们推荐的leader的状态不能有问题
        if (voteSet.hasAllQuorums() && checkLeader(outofelection, n.leader, n.electionEpoch)) {
            synchronized (this) {
                logicalclock.set(n.electionEpoch);
                setPeerState(n.leader, voteSet);
            }
            Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
            leaveInstance(endVote);
            return endVote;
        }

        return null;
    }

    private Vote receivedLeadingNotification(Map<Long, Vote> recvset, Map<Long, Vote> outofelection, SyncedLearnerTracker voteSet, Notification n) {
        /*
         *
         * In a two-node configuration, a recovery nodes cannot locate a leader because of the lack of the majority in the voteset.
         * Therefore, it is the time for Oracle to take place as a tight breaker.
         *
         * */
        // 校验当前server能否承认找到leading推荐的leader
        Vote result = receivedFollowingNotification(recvset, outofelection, voteSet, n);
        if (result == null) {
            /*
             * Ask Oracle to see if it is okay to follow this leader.
             *
             * We don't need the CheckLeader() because itself cannot be a leader candidate
             * */
            // 要求Oracle查看是否可以跟从该领导者。
            //  我们不需要CheckLeader（），因为它本身不能成为领导者候选人
            if (self.getQuorumVerifier().getNeedOracle() && !self.getQuorumVerifier().askOracle()) {
                LOG.info("Oracle indicates to follow");
                setPeerState(n.leader, voteSet);
                Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                leaveInstance(endVote);
                return endVote;
            } else {
                LOG.info("Oracle indicates not to follow");
                return null;
            }
        } else {
            // 能接收就代表找到了leader
            return result;
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return self.getCurrentAndNextConfigVoters().contains(sid);
    }

}
