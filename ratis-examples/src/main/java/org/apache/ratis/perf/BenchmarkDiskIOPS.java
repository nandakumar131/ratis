package org.apache.ratis.perf;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogOutputStream;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkDiskIOPS {

  private static final ClientId CLIENT_ID = ClientId.randomId();
  private static final AtomicLong CALL_ID = new AtomicLong();

  private final File dir;
  private final ByteBuffer writeBuffer;
  private final long segmentMaxSize;
  private final long preallocatedSize;
  private final long maxFileSize;

  private volatile File openFile;
  private volatile SegmentedRaftLogOutputStream out;

  private long totalRecordsWritten = 0;

  public BenchmarkDiskIOPS(File dir) {

    final int bufferSize = RaftServerConfigKeys.Log.WRITE_BUFFER_SIZE_DEFAULT.getSizeInt();

    this.dir = dir;
    this.writeBuffer = ByteBuffer.allocateDirect(bufferSize);
    this.segmentMaxSize = RaftServerConfigKeys.Log.SEGMENT_SIZE_MAX_DEFAULT.getSize();
    this.preallocatedSize = RaftServerConfigKeys.Log.PREALLOCATED_SIZE_DEFAULT.getSize();
    this.maxFileSize = SizeInBytes.valueOf("48MB").getSize();
  }


  public void run() throws IOException {
    List<SegmentRange> segmentRanges = prepareRanges(0, 5, 1000, 0);
    final byte[] content = new byte[1024];
    List<RaftProtos.LogEntryProto> entries = prepareLogEntries(segmentRanges);
    for (RaftProtos.LogEntryProto entry : entries) {
      write(entry);
    }
  }

  public void write(RaftProtos.LogEntryProto entry) throws IOException {
    if (openFile == null || openFile.length() >= maxFileSize) {
      allocateNewFile();
    }
    out.write(entry);
    flushIfNecessary();
  }

  private void allocateNewFile() throws IOException {
    openFile =  new File(dir, UUID.randomUUID().toString() + ".log");
    allocateSegmentedRaftLogOutputStream(openFile, false);
  }

  private void allocateSegmentedRaftLogOutputStream(File file, boolean append) throws
      IOException {
    out = new SegmentedRaftLogOutputStream(file, append, segmentMaxSize, preallocatedSize, writeBuffer);
  }

  static List<SegmentRange> prepareRanges(int startTerm, int endTerm, int segmentSize,
                                          long startIndex) {
    List<SegmentRange> list = new ArrayList<>(endTerm - startTerm);
    for (int i = startTerm; i < endTerm; i++) {
      list.add(new SegmentRange(startIndex, startIndex + segmentSize - 1, i));
      startIndex += segmentSize;
    }
    return list;
  }

  static List<RaftProtos.LogEntryProto> prepareLogEntries(List<SegmentRange> slist) {
    List<RaftProtos.LogEntryProto> eList = new ArrayList<>();
    for (SegmentRange range : slist) {
      for(long index = range.start; index <= range.end; index++) {
        eList.add(prepareLogEntry(range.term, index));
      }
    }
    return eList;
  }

  static RaftProtos.LogEntryProto prepareLogEntry(long term, long index) {
    final ByteString bytes = ProtoUtils.toByteString("m" + index);
    RaftProtos.StateMachineLogEntryProto smLogEntryProto = LogProtoUtils.toStateMachineLogEntryProto(
        CLIENT_ID, CALL_ID.incrementAndGet(), RaftProtos.StateMachineLogEntryProto.Type.WRITE, bytes, null);
    return LogProtoUtils.toLogEntryProto(smLogEntryProto, term, index);
  }

  static class SegmentRange {
    final long start;
    final long end;
    final long term;

    SegmentRange(long s, long e, long term) {
      this.start = s;
      this.end = e;
      this.term = term;
    }
  }

  private void flushIfNecessary() throws IOException {
    if (shouldFlush()) {
      out.flush();
      totalRecordsWritten++;
    }
  }

  private boolean shouldFlush() {
    return out != null;
  }

  public static void main(String[] args) throws IOException {
    BenchmarkDiskIOPS biops = new BenchmarkDiskIOPS(new File(args[0]));

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      biops.run();
    }
    long endTime = System.currentTimeMillis();

    System.out.println("Total time: " + (endTime - startTime));
    System.out.println("Number of transactions: " + biops.totalRecordsWritten);
    long timeInSec = ((endTime - startTime) / 1000);
    System.out.println("IOPS " + (biops.totalRecordsWritten / timeInSec));

  }
}
