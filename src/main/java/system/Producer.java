package system;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import util.Constants;
import util.Util;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
  private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);

  private final Integer producerId;
  private final RocksDB producerDb;
  private final RocksDB taskDb;
  private int nextOffset; // Note: must match default value for an offset if no offset file found

  private final AtomicInteger firstReplicaPendingCommit = new AtomicInteger();
  private final AtomicInteger secondReplicaPendingCommit = new AtomicInteger();

  public Producer(Integer producerId, RocksDB producerDb, RocksDB taskDb) {
    this.producerId = producerId;
    this.producerDb = producerDb;
    this.taskDb = taskDb;
    this.nextOffset = Ints.fromByteArray(Util.readFile(Constants.Common.getProducerOffsetFilePath(producerId)));
    LOGGER.info("Restoring next offset to: {} for Producer: {}", nextOffset, producerId);
  }

  public void start() {
    LOGGER.info("Producer: {} is now running.", producerId);
    String firstReplicaId = producerId + "0";
    Integer firstReplicaPort = Constants.Common.JOB_MODEL.getReplicators().get(firstReplicaId).right;
    new ReplicaConnection(producerId, firstReplicaId, firstReplicaPort, producerDb, taskDb,
        firstReplicaPendingCommit).start();

    String secondReplicaId = producerId + "1";
    Integer secondReplicaPort = Constants.Common.JOB_MODEL.getReplicators().get(secondReplicaId).right;
    new ReplicaConnection(producerId, secondReplicaId, secondReplicaPort, producerDb, taskDb,
        secondReplicaPendingCommit).start();
  }

  // TODO HOW handle DELETE?
  public void send(byte[] key, byte[] value) throws Exception {
    byte[] message = ByteBuffer.wrap(new byte[8]).put(key).put(value).array();
    producerDb.put(Ints.toByteArray(nextOffset), message);
    nextOffset++;
  }

  public void commit() throws Exception {
    int commitOffset = nextOffset - 1;
    producerDb.flush(Constants.Common.FLUSH_OPTIONS);
    firstReplicaPendingCommit.incrementAndGet();
    secondReplicaPendingCommit.incrementAndGet();
    while (firstReplicaPendingCommit.get() != 0 || secondReplicaPendingCommit.get() != 0) {
      LOGGER.debug("firstReplicaPendingCommit: {}, secondReplicaPendingCommit: {}", firstReplicaPendingCommit.get(), secondReplicaPendingCommit.get());
      Thread.sleep(10); // wait for commit ack
    }

    // may be less than the offset actually committed at the replicator
    Util.writeFile(Constants.Common.getProducerOffsetFilePath(producerId), Ints.toByteArray(commitOffset));

    // clean up data up to committed offset
    RocksIterator iterator = producerDb.newIterator();
    iterator.seekToFirst();
    byte[] dbKey = iterator.key();
    LOGGER.debug("Trimming producerDb from oldest offset: {} to committed offset: {} in Producer: {}",
        Ints.fromByteArray(dbKey), commitOffset, producerId);
    producerDb.deleteRange(dbKey, Ints.toByteArray(commitOffset + 1)); // should be inclusive of committed offset
    iterator.close();
  }

  private static class ReplicaConnection extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaConnection.class);
    private final Integer producerId;
    private final String replicatorId;
    private final Integer replicaPort;
    private final RocksDB producerDb;
    private final RocksDB taskDb;
    private final AtomicInteger pendingCommit;

    public ReplicaConnection(Integer producerId, String replicatorId, Integer replicaPort,
        RocksDB producerDb, RocksDB taskDb, AtomicInteger pendingCommit) {
      super("ReplicaConnection " + replicatorId);
      this.producerId = producerId;
      this.replicatorId = replicatorId;
      this.replicaPort = replicaPort;
      this.producerDb = producerDb;
      this.taskDb = taskDb;
      this.pendingCommit = pendingCommit;
    }

    public void run() {
      LOGGER.info("ReplicaConnection handler to Replicator: {} for Producer: {} is now running.", replicatorId, producerId);
      try {
        Socket socket = new Socket();
        socket.setTcpNoDelay(true);

        while (!socket.isConnected()) {
          try {
            socket.connect(new InetSocketAddress(Constants.Common.SERVER_HOST, replicaPort), 0);
            LOGGER.info("Connected to Replicator: {} in Producer: {}", replicatorId, producerId);
            replicate(socket); // blocks
          } catch (Exception ce) {
            LOGGER.debug("Retrying connection to Replicator: {} in Producer: {}", replicatorId, producerId);
            socket = new Socket();
            Thread.sleep(1000);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Error in ReplicaConnection to Replicator: " + replicatorId + " in Producer: " + producerId, e);
      }
    }

    private void replicate(Socket socket) throws Exception {
      DataInputStream inputStream = new DataInputStream(socket.getInputStream());
      OutputStream outputStream = socket.getOutputStream();

      int lastCommittedOffset = getLastCommittedOffset(inputStream, outputStream);
      LOGGER.debug("Last committed offset before synchronization: {} for Replicator: {} in Producer: {}",
          lastCommittedOffset, replicatorId, producerId);

      int producerLCOffset = Ints.fromByteArray(Util.readFile(Constants.Common.getProducerOffsetFilePath(producerId)));
      if (lastCommittedOffset < producerLCOffset) {
        LOGGER.info("Replica: {} LCO: {} was less than producer LCO: {}", replicatorId, lastCommittedOffset, producerLCOffset);
        deleteReplica(inputStream, outputStream);
        taskDb.flush(Constants.Common.FLUSH_OPTIONS);
        writeTaskDb(outputStream); // send everything in task db.
        lastCommittedOffset = 0; // send everything in producer db.
      }

      producerDb.flush(Constants.Common.FLUSH_OPTIONS);
      byte[] lastSentOffset = writeSinceOffset(outputStream, Ints.toByteArray(lastCommittedOffset));
      LOGGER.debug("Last sent offset after synchronization: {} for Replicator: {} in Producer: {}",
          Ints.fromByteArray(lastSentOffset), replicatorId, producerId);

      while(!Thread.currentThread().isInterrupted()) {
        byte[] sentOffset = writeSinceOffset(outputStream, lastSentOffset);
        if (!Arrays.equals(sentOffset, lastSentOffset)) { // if equal, didn't send anything new, don't increment
          lastSentOffset = Ints.toByteArray(Ints.fromByteArray(sentOffset) + 1);
        }
        Thread.sleep(10);

        if (pendingCommit.get() == 1) {
          lastSentOffset = writeSinceOffset(outputStream, lastSentOffset);
          commit(inputStream, outputStream, lastSentOffset);
          pendingCommit.decrementAndGet();
        }
      }
    }

    // returns the last offset committed at the replicator
    private int getLastCommittedOffset(DataInputStream inputStream, OutputStream outputStream) throws Exception {
      final byte[] opCode = new byte[4];
      final byte[] offset = new byte[4];

      LOGGER.debug("Requesting LCO from Replicator: {} in Producer: {}", replicatorId, producerId);
      outputStream.write(Constants.Common.OPCODE_LCO);
      outputStream.flush();

      inputStream.readFully(opCode);
      if (!Arrays.equals(opCode, Constants.Common.OPCODE_LCO)) {
        throw new IllegalStateException();
      }
      inputStream.readFully(offset);
      LOGGER.debug("Received LCO: {} from Replicator: {} in Producer: {}",
          Ints.fromByteArray(offset), replicatorId, producerId);
      return Ints.fromByteArray(offset);
    }

    // returns the last offset sent to replicator (may not be committed)
    private byte[] writeSinceOffset(OutputStream outputStream, byte[] offset) throws IOException {
      byte[] lastSentOffset = offset;
      // send data from DB since provided offset.
      RocksIterator iterator = producerDb.newIterator();
      iterator.seek(offset);
      while(iterator.isValid()) {
        byte[] storedOffset = iterator.key();
        byte[] message = iterator.value();
        byte[] messageKey = new byte[4];
        byte[] messageValue = new byte[4];
        ByteBuffer.wrap(message).get(messageKey);
        ByteBuffer.wrap(message).get(messageValue);

        LOGGER.debug("Sending data for offset: {} key: {} to Replicator: {} from Producer: {}",
            Ints.fromByteArray(storedOffset), Ints.fromByteArray(messageKey), replicatorId, producerId);
        outputStream.write(Constants.Common.OPCODE_WRITE);
        outputStream.write(messageKey);
        outputStream.write(messageValue);
        lastSentOffset = storedOffset;
        iterator.next();
      }
      outputStream.flush();
      iterator.close();
      return lastSentOffset;
    }

    // commit provided offset at replicator
    private void commit(DataInputStream inputStream, OutputStream outputStream, byte[] offset) throws Exception {
      byte[] opCode = new byte[4];
      byte[] committedOffset = new byte[4];
      LOGGER.debug("Requesting commit for offset: {} to Replicator: {} in Producer: {}",
          Ints.fromByteArray(offset), replicatorId, producerId);
      outputStream.write(Constants.Common.OPCODE_COMMIT);
      outputStream.write(offset);
      outputStream.flush();

      inputStream.readFully(opCode);
      if (!Arrays.equals(opCode, Constants.Common.OPCODE_COMMIT)) {
        throw new IllegalStateException("Illegal opCode: " + Ints.fromByteArray(opCode) +
            " from Replicator: " + replicatorId + " in Producer: " + producerId);
      }
      inputStream.readFully(committedOffset);
      LOGGER.debug("Received commit acknowledgement for offset: {} from Replicator: {} in Producer: {}",
          Ints.fromByteArray(offset), replicatorId, producerId);
      Preconditions.checkState(Arrays.equals(offset, committedOffset));
    }

    // deleteReplica everything in replicator db
    private void deleteReplica(DataInputStream inputStream, OutputStream outputStream) throws Exception {
      byte[] opCode = new byte[4];
      LOGGER.info("Requesting delete from Replicator: {} in Producer: {}", replicatorId, producerId);
      outputStream.write(Constants.Common.OPCODE_DELETE);
      outputStream.flush();

      inputStream.readFully(opCode);
      if (!Arrays.equals(opCode, Constants.Common.OPCODE_DELETE)) {
        throw new IllegalStateException("Illegal opCode: " + Ints.fromByteArray(opCode) +
            " from Replicator: " + replicatorId + " in Producer: " + producerId);
      }
      LOGGER.info("Received delete acknowledgement from Replicator: {} in Producer: {}", replicatorId, producerId);
    }

    // send everything from task db
    private void writeTaskDb(OutputStream outputStream) throws IOException {
      LOGGER.info("Sending data from taskDb to Replicator: {} from Producer: {}", replicatorId, producerId);
      RocksIterator iterator = taskDb.newIterator();
      iterator.seekToFirst();
      int numMessagesSent = 0;
      while(iterator.isValid()) {
        byte[] message = iterator.value();
        byte[] messageKey = new byte[4];
        byte[] messageValue = new byte[4];
        ByteBuffer.wrap(message).get(messageKey);
        ByteBuffer.wrap(message).get(messageValue);

        LOGGER.debug("Sending data from taskDb for key: {} to Replicator: {} from Producer: {}",
            Ints.fromByteArray(messageKey), replicatorId, producerId);
        outputStream.write(Constants.Common.OPCODE_WRITE);
        outputStream.write(messageKey);
        outputStream.write(messageValue);
        numMessagesSent++;
        iterator.next();
      }
      outputStream.flush();
      iterator.close();
      LOGGER.info("Sent {} messages from taskDb to Replicator: {} from Producer: {}", numMessagesSent, replicatorId, producerId);
    }
  }
}
