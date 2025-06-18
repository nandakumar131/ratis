package org.apache.ratis.perf;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SimpleMain {

  private static final SecureRandom numberGenerator = new SecureRandom();

  public static class SimpleServer {

    private final SimpleRatisServer server;

    public SimpleServer(String peerId, int port, String peers, String storageDir)
        throws IOException {
      this.server = new SimpleRatisServer(peerId, port, peers, storageDir);
    }

    public void start() throws IOException {
      server.start();
    }

    public void stop() throws IOException, InterruptedException {
      server.join();
      server.stop();
    }
  }

  public static class SimpleClient {
    private final String peers;
    private final int threads;
    private final int transactionCount;
    private final Map<Integer, ByteString> randomData;

    public SimpleClient(String peers, int threads, int transactionCount) {
      this.peers = peers;
      this.threads = threads;
      this.transactionCount = transactionCount;
      this.randomData = new HashMap<>();
    }

    private void populateRandomData() {
      for (int i = 0; i < 100; i++) {
        //final ByteString base = ByteString.copyFromUtf8(UUID.randomUUID().toString());
        randomData.put(i, ByteString.copyFrom(randomBytes(4 * 1024)));
      }
    }

    public void submitDirect(SimpleRatisServer server) throws Exception {
      populateRandomData();
      long start = System.currentTimeMillis();
      System.out.println("Starting server...");
      List<Future<Void>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(CompletableFuture.runAsync(() -> runBatch(server)));
      }
      for (Future<Void> future : futures) {
        future.get();
      }
      System.out.println("la fin!");
      long end = System.currentTimeMillis();
      System.out.println("Total time taken: " + (end - start) + " ms");
    }

    public void runBatch(SimpleRatisServer server) {
      int randomDataSize = randomData.size();
      try {
        for (int i = 0; i < transactionCount; i++) {
          server.submitRequest(randomData.get(i % randomDataSize));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    public void run() throws Exception {
      populateRandomData();
      long start = System.currentTimeMillis();
      System.out.println("Starting server...");
      List<Future<Void>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(CompletableFuture.runAsync(() -> runBatch(getClient())));
      }
      for (Future<Void> future : futures) {
        future.get();
      }
      System.out.println("la fin!");
      long end = System.currentTimeMillis();
      System.out.println("Total time taken: " + (end - start) + " ms");
    }

    public void runBatch(RaftClient client) {
      int randomDataSize = randomData.size();
      try {
        for (int i = 0; i < transactionCount; i++) {
          client.io().send(Message.valueOf(randomData.get(i % randomDataSize)));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    private RaftClient getClient() {
      RaftProperties raftProperties = getClientProperties();
      final RaftGroup raftGroup = buildRaftGroup(getRaftGroupId(), peers);

      RaftClient.Builder builder =
          RaftClient.newBuilder().setProperties(raftProperties);
      builder.setRaftGroup(raftGroup);
      builder.setClientRpc(
          new GrpcFactory(new org.apache.ratis.conf.Parameters())
              .newRaftClientRpc(ClientId.randomId(), raftProperties));
      return builder.build();
    }

    private static RaftProperties getClientProperties() {
      int raftSegmentPreallocatedSize = 1024 * 1024 * 1024;
      RaftProperties raftProperties = new RaftProperties();
      RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.GRPC);
      GrpcConfigKeys.setMessageSizeMax(raftProperties,
          SizeInBytes.valueOf(raftSegmentPreallocatedSize));
      RaftServerConfigKeys.Log.Appender.setBufferByteLimit(raftProperties,
          SizeInBytes.valueOf(raftSegmentPreallocatedSize));
      RaftServerConfigKeys.Log.setWriteBufferSize(raftProperties,
          SizeInBytes.valueOf(raftSegmentPreallocatedSize));
      RaftServerConfigKeys.Log.setPreallocatedSize(raftProperties,
          SizeInBytes.valueOf(raftSegmentPreallocatedSize));
      RaftServerConfigKeys.Log.setSegmentSizeMax(raftProperties,
          SizeInBytes.valueOf(1 * 1024 * 1024 * 1024L));
      RaftConfigKeys.DataStream.setType(raftProperties, SupportedDataStreamType.NETTY);
      RaftServerConfigKeys.Log.setSegmentCacheNumMax(raftProperties, 2);
      RaftClientConfigKeys.Rpc.setRequestTimeout(raftProperties,
          TimeDuration.valueOf(50000, TimeUnit.MILLISECONDS));
      RaftClientConfigKeys.Async.setOutstandingRequestsMax(raftProperties, 1000);
      return raftProperties;
    }
  }

  public static byte[] randomBytes(int size) {
    byte[] randomBytes = new byte[size];
    numberGenerator.nextBytes(randomBytes);
    return randomBytes;
  }

  public static RaftGroupId getRaftGroupId() {
    return RaftGroupId.valueOf(UUID.fromString("152547C8-B868-42E0-901E-634A075C05EE"));
  }

  public static RaftGroup buildRaftGroup(RaftGroupId groupId , String peerAddress) {
    final List<RaftPeer> raftPeers = new ArrayList<>();
    final RaftPeerId[] peerIds = new RaftPeerId[3];

    peerIds[0] = RaftPeerId.getRaftPeerId("peer-one");
    peerIds[1] = RaftPeerId.getRaftPeerId("peer-two");
    peerIds[2] = RaftPeerId.getRaftPeerId("peer-three");

    final String[] addresses = peerAddress.split(",");

    for (int i = 0; i < addresses.length; i++) {
      final RaftPeer peer = RaftPeer.newBuilder().setId(peerIds[i]).setAddress(addresses[i]).build();
      raftPeers.add(peer);
    }
    return RaftGroup.valueOf(groupId, raftPeers);
  }

  public static void main(String[] args) throws Exception {

    if (args.length == 0) {
      System.out.println("Usage: SimpleMain <server | client>");
      return;
    }

    final String role = args[0];
    if (!role.equals("server") && !role.equals("client") && !role.equals("standalone")) {
      System.out.println("Usage: SimpleMain <server | client | standalone>");
      return;
    }

    if (role.equals("server")) {
      if(args.length != 5) {
        System.out.println(
            "Usage: SimpleMain server <peer-id> <port> <comma seperated list of peers> <storageDir>");
        return;
      }
      String peerId = args[1];
      int port = Integer.parseInt(args[2]); // 9898
      String peers = args[3];
      String storageDir = args[4]; // /Users/nvadivelu/Workspace/Ratis/Data
      SimpleServer server = new SimpleServer(peerId, port, peers, storageDir);
      server.start();
      server.stop();
    }


    if (role.equals("client")) {
      if (args.length != 4) {
        System.out.println("Usage: SimpleMain client <peers> <threadCount> <transactionsPerThread>");
        return;
      }
      String peers = args[1];
      int numThreads = Integer.parseInt(args[2]); // 10
      int numberOfTransactionsPerThread = Integer.parseInt(args[3]); // 1_000_000
      SimpleClient client = new SimpleClient(peers, numThreads, numberOfTransactionsPerThread);
      client.run();
    }

    if (role.equals("standalone")) {
      if(args.length != 7) {
        System.out.println(
            "Usage: SimpleMain standalone <peer-id> <port> <comma seperated list of peers> <storageDir> <threadCount> <transactionsPerThread>");
        return;
      }
      String peerId = args[1];
      int port = Integer.parseInt(args[2]); // 9898
      String peers = args[3];
      String storageDir = args[4]; // /Users/nvadivelu/Workspace/Ratis/Data
      int numThreads = Integer.parseInt(args[5]); // 10
      int numberOfTransactionsPerThread = Integer.parseInt(args[6]); // 1_000_000
      SimpleServer server = new SimpleServer(peerId, port, peers, storageDir);
      SimpleClient client = new SimpleClient(peers, numThreads, numberOfTransactionsPerThread);
      server.start();
      client.submitDirect(server.server);
      server.stop();
    }
  }
}
