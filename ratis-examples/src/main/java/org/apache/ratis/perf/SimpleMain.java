package org.apache.ratis.perf;

import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class SimpleMain {

  private static final SecureRandom numberGenerator = new SecureRandom();

  private final SimpleRatisServer server;
  private final String peerID;

  private final Map<Integer, ByteString> randomData;

  public SimpleMain(String peerId, int port, String peers, String storageDir) throws IOException {
    this.peerID = peerId;
    this.server = new SimpleRatisServer(peerId, port, peers, storageDir);
    this.randomData = new HashMap<>();
  }

  public void start() throws IOException {
    server.start();
    if (peerID.equals("peer-one")) {
      server.waitForLeaderToBeReady();
    }
  }

  public void run(int threadCount, int transactionsPerThread) throws Exception {
    populateRandomData();
    long start = System.currentTimeMillis();
    System.out.println("Starting server...");
    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      futures.add(CompletableFuture.runAsync(() -> runBatch(transactionsPerThread)));
    }
    for (Future<Void> future : futures) {
      future.get();
    }
    long end = System.currentTimeMillis();
    System.out.println("la fin!");
    System.out.println("Total time taken: " + (end - start) + " ms");
  }

  private void populateRandomData() {
    for (int i = 0; i < 100; i++) {
      //final ByteString base = ByteString.copyFromUtf8(UUID.randomUUID().toString());
      randomData.put(i, ByteString.copyFrom(randomBytes(1)));
    }
  }

  public static byte[] randomBytes(int size) {
    byte[] randomBytes = new byte[size];
    numberGenerator.nextBytes(randomBytes);
    return randomBytes;
  }

  public void runBatch(int transactionsPerThread) {
    int randomDataSize = randomData.size();
    try {
      for (int i = 0; i < transactionsPerThread; i++) {
        server.submitRequest(randomData.get(i % randomDataSize));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void stop() throws IOException, InterruptedException {
    server.join();
    server.stop();
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 6) {
      System.out.println("Usage: SimpleMain <peer-id> <port> <comma seperated list of peers> <storageDir> <threadCount> <transactionsPerThread>");
      return;
    }
    String peerId = args[0];
    int port = Integer.parseInt(args[1]); // 9898
    String peers = args[2];
    String storageDir = args[3]; // /Users/nvadivelu/Workspace/Ratis/Data
    int numThreads = Integer.parseInt(args[4]); // 10
    int numberOfTransactionsPerThread = Integer.parseInt(args[5]); // 1_000_000
    SimpleMain main = new SimpleMain(peerId, port, peers, storageDir);
    main.start();
    if (peerId.equals("peer-one")) {
      main.run(numThreads, numberOfTransactionsPerThread);
    }
    main.stop();

  }
}