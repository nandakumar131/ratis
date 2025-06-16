/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratis.perf;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

public class SimpleRatisServer {

  private final RaftProperties properties;
  private final RaftPeerId peerId;
  private final RaftGroupId raftGroupId;
  private final RaftServer raftServer;
  private final RaftServer.Division division;
  private final AtomicLong callId;
  private final ClientId clientId;

  public SimpleRatisServer(String peerId, int port, String peerAddress,
                           String storageDir) throws IOException {
    this.storageDir = storageDir;
    this.port = port;
    this.properties = getRaftProperties(storageDir, port);
    this.peerId = RaftPeerId.getRaftPeerId(peerId);
    this.raftGroupId = RaftGroupId.valueOf(
        UUID.fromString("152547C8-B868-42E0-901E-634A075C05EE"));
    RaftGroup group = buildRaftGroup(raftGroupId, peerAddress);
    this.raftServer = getRaftServer(group);
    this.division = raftServer.getDivision(raftGroupId);
    this.callId = new AtomicLong();
    this.clientId = ClientId.randomId();

  }

  public void start() throws IOException {
    raftServer.start();
  }

  public void join() throws InterruptedException {
    for(; raftServer.getLifeCycleState()!=LifeCycle.State.CLOSED; ) {
      TimeUnit.SECONDS.sleep(1);
    }
  }

  public void stop() throws IOException {
    raftServer.close();
  }

  public boolean submitRequest(final ByteString request)
      throws IOException {
    final RaftClientRequest raftClientRequest = RaftClientRequest.newBuilder()
        .setClientId()
        .setServerId(peerId)
        .setGroupId(raftGroupId)
        .setCallId(nextCallId())
        .setMessage(Message.valueOf(request))
        .setType(RaftClientRequest.writeRequestType())
        .build();
    long start = System.currentTimeMillis();
    final RaftClientReply raftClientReply = raftServer.submitClientRequest(raftClientRequest);
    long end = System.currentTimeMillis();
    return raftClientReply.isSuccess();
  }

  private RaftServer getRaftServer(RaftGroup group) throws IOException {
    return RaftServer.newBuilder().setServerId(peerId)
        .setProperties(properties)
        .setStateMachine(new SimpleStateMachine())
        .setOption(RaftStorage.StartupOption.RECOVER)
        .setGroup(group)
        .build();
  }

  private static RaftProperties getRaftProperties(String storageDir, int port) {
    final RaftProperties prop = new RaftProperties();
    final SizeInBytes logAppenderQueueByteLimit = SizeInBytes.valueOf("32m");

    RaftServerConfigKeys.setStorageDir(prop, Collections.singletonList(new File(storageDir)));
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("48m"));
    RaftServerConfigKeys.Log.setWriteBufferSize(prop, SizeInBytes.valueOf(logAppenderQueueByteLimit.getSize() + SizeInBytes.ONE_MB.getSize()));
    RaftServerConfigKeys.Log.setPreallocatedSize(prop, SizeInBytes.valueOf("4m"));

    RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(prop, false);
    RaftServerConfigKeys.Log.setPurgeGap(prop, 1000000);
    RaftServerConfigKeys.Log.setSegmentCacheNumMax(prop, 2);

    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(prop, false);
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(prop, 1024);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(prop, logAppenderQueueByteLimit);
    RaftServerConfigKeys.RetryCache.setExpiryTime(prop, TimeDuration.valueOf(60 * 1000L, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(prop, false);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(prop, 1000000L);
    RaftServerConfigKeys.LeaderElection.setPreVote(prop, true);

    RaftConfigKeys.Rpc.setType(prop, RpcType.valueOf("GRPC"));

    GrpcConfigKeys.Server.setPort(prop, port);
    GrpcConfigKeys.setMessageSizeMax(prop, SizeInBytes.valueOf(logAppenderQueueByteLimit.getSize() + SizeInBytes.ONE_MB.getSize()));

    //RaftServerConfigKeys.Log.setLogMetadataEnabled(prop, false);
    //RaftServerConfigKeys.Log.setUnsafeFlushEnabled(prop, true);

    return prop;
  }

  private RaftGroup buildRaftGroup(RaftGroupId groupId , String peerAddress) {
    final List<RaftPeer> raftPeers = new ArrayList<>();
    final RaftPeerId[] peerIds = new RaftPeerId[3];

    peerIds[0] = RaftPeerId.getRaftPeerId("peer-one");
    peerIds[1] = RaftPeerId.getRaftPeerId("peer-two");
    peerIds[2] = RaftPeerId.getRaftPeerId("peer-three");

    final String[] addresses = peerAddress.split(",");

    for (int i = 0; i < addresses.length; i++) {
      System.out.println(addresses[i]);
      final RaftPeer peer = RaftPeer.newBuilder().setId(peerIds[i]).setAddress(addresses[i]).build();
      raftPeers.add(peer);
    }
    return RaftGroup.valueOf(groupId, raftPeers);
  }

  public void waitForLeaderToBeReady() throws IOException {
    boolean ready;
    do {
      ready = division.getInfo().isLeaderReady();
      if (!ready) {
        try {
          System.out.println("waiting for cluster to be ready");
          System.out.println(getRatisRoles());
          Thread.sleep( 2 * 1000L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    } while (!ready);
    System.out.println("Cluster ready!");
  }

  public List<String> getRatisRoles() throws IOException {
    Collection<RaftPeer> peers = division.getGroup().getPeers();
    RaftPeer leader = getLeader();
    List<String> ratisRoles = new ArrayList<>();
    for (RaftPeer peer : peers) {
      InetAddress peerInetAddress = null;
      ratisRoles.add((peer.getAddress() == null ? "" :
          peer.getAddress().concat(peer.equals(leader) ?
                  ":".concat(RaftProtos.RaftPeerRole.LEADER.toString()) :
                  ":".concat(RaftProtos.RaftPeerRole.FOLLOWER.toString()))
              .concat(":".concat(peer.getId().toString()))));
    }
    return ratisRoles;
  }

  public RaftPeer getLeader() {
    if (division.getInfo().isLeader()) {
      return division.getPeer();
    } else {
      ByteString leaderId = division.getInfo().getRoleInfoProto()
          .getFollowerInfo().getLeaderInfo().getId().getId();
      return leaderId.isEmpty() ? null :
          division.getRaftConf().getPeer(RaftPeerId.valueOf(leaderId));
    }
  }

  private long nextCallId() {
    return callId.getAndIncrement() & Long.MAX_VALUE;
  }
}