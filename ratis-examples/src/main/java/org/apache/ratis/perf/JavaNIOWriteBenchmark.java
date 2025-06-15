package org.apache.ratis.perf;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.UUID;

public class JavaNIOWriteBenchmark {

  static final SecureRandom numberGenerator = new SecureRandom();

  private static final ByteBuffer FILL;
  private static final int BUFFER_SIZE = 1024 * 1024; // 1 MB
  private static final byte TERMINATOR_BYTE = 0;

  private final File dir;
  private final ByteBuffer buffer;
  private final long maxFileSize;
  private final long preAllocatedSize;
  private final boolean preallocate;
  private final boolean force;

  private RandomAccessFile raf;
  private FileChannel fc;
  private long totalRecordsWritten = 0;

  static {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    for (int i = 0; i < BUFFER_SIZE; i++) {
      buffer.put(TERMINATOR_BYTE);
    }
    buffer.flip();
    FILL = buffer.asReadOnlyBuffer();
  }

  public JavaNIOWriteBenchmark(File dir, String sync, String preallocate) throws Exception {
    this.preAllocatedSize = mbToBytes(4);
    this.maxFileSize = mbToBytes(48);
    this.dir = dir;
    this.force = Boolean.parseBoolean(sync);
    this.preallocate = Boolean.parseBoolean(preallocate);
    byte[] base = randomBytes(3000);
    this.buffer = ByteBuffer.allocate(base.length + 1000);
    this.buffer.put(base);

    System.out.println("Output directory: " + dir);
    System.out.println("File size: " + maxFileSize);
    System.out.println("Sync: " + force);
    System.out.println("Preallocate: " + preallocate);
    if (this.preallocate) {
      System.out.println("Preallocate size: " + preAllocatedSize);
    }
  }

  private long mbToBytes(long mb) {
    return mb * 1024 * 1024;
  }

  public void run(int numberOfRecords) throws Exception {
    for (int i = 0; i < numberOfRecords; i++) {
      byte[] randomBytes = new byte[1000];
      numberGenerator.nextBytes(randomBytes);
      buffer.position(2999);
      buffer.put(randomBytes);
      buffer.flip();
      writeToChannel(buffer);
    }
  }

  void writeToChannel(ByteBuffer buffer) throws IOException {
    if (raf == null || fc.size() >= maxFileSize) {
      allocateNewFile();
    }
    preallocateIfNecessary(buffer.remaining());
    final int length = buffer.remaining();
    int written = 0;
    while (written < length) {
      written += fc.write(buffer);
    }
    if (force) {
      fc.force(false);
    }
    totalRecordsWritten++;
  }

  void preallocateIfNecessary(long size) throws IOException {
    if (preallocate && fc.position() + size > fc.size()) {
        preallocate(size);
    }
  }

  private void preallocate(long outstanding) throws IOException {
    final long size = fc.size();
    final long actual = actualPreallocateSize(outstanding, maxFileSize - size, preAllocatedSize);
    preallocate(fc, actual);
  }

  private static long actualPreallocateSize(long outstandingData, long remainingSpace, long preallocate) {
    return outstandingData > remainingSpace? outstandingData
        : outstandingData > preallocate? outstandingData
        : Math.min(preallocate, remainingSpace);
  }

  static void preallocate(FileChannel fc, long size) throws IOException {
    final int remaining = FILL.remaining();
    long allocated = 0;
    while (allocated < size) {
      final long required = size - allocated;
      final int n = remaining < required? remaining: Math.toIntExact(required);
      final ByteBuffer buffer = FILL.slice();
      buffer.limit(n);
      final int written = fc.write(buffer, fc.size());
      allocated += written;
    }
  }

  private void allocateNewFile() throws IOException {
    File openFile =  new File(dir, UUID.randomUUID().toString() + ".log");
    this.raf = new RandomAccessFile(openFile, "rw");
    this.fc = raf.getChannel();
  }

  public static byte[] randomBytes(int size) {
    byte[] randomBytes = new byte[size];
    numberGenerator.nextBytes(randomBytes);
    return randomBytes;
  }

  void close() throws IOException {
    raf.close();
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 4) {
      System.out.println("Usage: java JavaNIOWriteBenchmark [dir] [sync] [preallocate] [numberOfRecords]");
    }
    JavaNIOWriteBenchmark jniob = new JavaNIOWriteBenchmark(new File(args[0]), args[1], args[2]);
    long start = System.currentTimeMillis();
    jniob.run(Integer.parseInt(args[3]));
    long end = System.currentTimeMillis();
    jniob.close();

    System.out.println("Total time: " + (end - start));
    System.out.println("Number of transactions: " + jniob.totalRecordsWritten);
    long timeInSec = ((end - start) / 1000);
    System.out.println("IOPS " + (jniob.totalRecordsWritten / timeInSec));
  }
}
