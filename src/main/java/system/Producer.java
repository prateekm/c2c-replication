package system;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import util.Constants;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
  private static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
  private static final FlushOptions FLUSH_OPTIONS = new FlushOptions().setWaitForFlush(true);

  private final Integer producerId;
  private final RocksDB producerDb;
  private final Integer firstReplicaPort;
  private final Integer secondReplicaPort;
  private int lastOffset = 1; // TODO: must match default value in offset file

  private final AtomicInteger firstReplicaPendingCommit = new AtomicInteger();
  private final AtomicInteger secondReplicaPendingCommit = new AtomicInteger();

  public Producer(Integer producerId, RocksDB producerDb, Integer firstReplicaPort, Integer secondReplicaPort) {
    this.producerId = producerId;
    this.producerDb = producerDb;
    this.firstReplicaPort = firstReplicaPort;
    this.secondReplicaPort = secondReplicaPort;

    try {
      RocksIterator iterator = producerDb.newIterator();
      iterator.seekToLast();
      if (iterator.isValid()) {
        lastOffset = Ints.fromByteArray(iterator.key()) + 1;
      } else {
        lastOffset = 1;
      }
    } catch (Exception e) {
      LOGGER.error("Could no get lastOffset from producerDb.", e);
      lastOffset = 1;
    }
    LOGGER.info("Restoring lastOffset to: {} for Producer: {}", lastOffset, producerId);
  }

  public void start() {
    LOGGER.info("Producer {} is now running.", producerId);
    new ReplicaConnection(producerId, firstReplicaPort, producerDb, firstReplicaPendingCommit).start();
    new ReplicaConnection(producerId, secondReplicaPort, producerDb, secondReplicaPendingCommit).start();
  }

  public void send(byte[] key, byte[] value) throws Exception {
    byte[] message = ByteBuffer.wrap(new byte[8]).put(key).put(value).array();
    producerDb.put(Ints.toByteArray(lastOffset), message);
    lastOffset++;
  }

  public void commit() throws Exception {
    producerDb.flush(FLUSH_OPTIONS);
    firstReplicaPendingCommit.incrementAndGet();
    secondReplicaPendingCommit.incrementAndGet();
    while (firstReplicaPendingCommit.get() != 0 || secondReplicaPendingCommit.get() != 0) {
      LOGGER.debug("firstReplicaPendingCommit: {}, secondReplicaPendingCommit: {}", firstReplicaPendingCommit.get(), secondReplicaPendingCommit.get());
      Thread.sleep(10); // wait for commit ack
    }

    // clean up data up to committed offset
    RocksIterator iterator = producerDb.newIterator();
    iterator.seekToFirst();
    byte[] dbKey = iterator.key();
    LOGGER.info("Requesting deleteRange from oldest offset: {} to committed offset: {} in Producer {}",
        Ints.fromByteArray(dbKey), lastOffset - 1, producerId);
    producerDb.deleteRange(dbKey, Ints.toByteArray(lastOffset + 1)); // should be inclusive of committed offset
    iterator.close();
  }

  private static class ReplicaConnection extends Thread {
    private final Integer producerId;
    private final Integer replicaPort;
    private final RocksDB producerDb;
    private final AtomicInteger pendingCommit;

    public ReplicaConnection(Integer producerId, Integer replicaPort, RocksDB producerDb, AtomicInteger pendingCommit) {
      this.producerId = producerId;
      this.replicaPort = replicaPort;
      this.producerDb = producerDb;
      this.pendingCommit = pendingCommit;
    }

    public void run() {
      LOGGER.info("ReplicaConnection handler to port: {} for Producer: {} is now running.", replicaPort, producerId);
      try {
        Socket socket = new Socket();
        socket.setTcpNoDelay(true);

        while (!socket.isConnected()) {
          try {
            socket.connect(new InetSocketAddress(Constants.SERVER_HOST, replicaPort), 0);
            LOGGER.info("Connected to {}:{} from Producer {}", Constants.SERVER_HOST, replicaPort, producerId);
            replicate(socket); // blocks
          } catch (Exception ce) {
            LOGGER.info("Retrying connection from Producer {}. Socket status: " + socket.toString(), producerId, ce);
            socket = new Socket();
            Thread.sleep(1000);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error creating socket connection from Producer {}", producerId, e);
      }
    }

    private void replicate(Socket socket) throws Exception {
      DataInputStream inputStream = new DataInputStream(socket.getInputStream());
      OutputStream outputStream = socket.getOutputStream();

      byte[] lastSentOffset = synchronize(inputStream, outputStream);
      LOGGER.info("Last offset after synchronization: {} in Producer: {}", Ints.fromByteArray(lastSentOffset), producerId);

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

    // returns the last offset written to replicator
    private byte[] synchronize(DataInputStream inputStream, OutputStream outputStream) throws Exception {
      final byte[] opCode = new byte[4];
      final byte[] offset = new byte[4];

      LOGGER.info("Requesting sync in Producer {}", producerId);
      outputStream.write(Constants.OPCODE_SYNC);
      outputStream.flush();

      inputStream.readFully(opCode);
      if (!Arrays.equals(opCode, Constants.OPCODE_SYNC)) {
        throw new IllegalStateException();
      }
      inputStream.readFully(offset);
      LOGGER.info("Received sync response with offset: {} in Producer: {}", Ints.fromByteArray(offset), producerId);

      producerDb.flush(FLUSH_OPTIONS);
      return writeSinceOffset(outputStream, offset);
    }

    // returns the last offset written to replicator
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

        LOGGER.debug("Sending data for offset: {} key: {} from Producer: {}",
            Ints.fromByteArray(storedOffset), Ints.fromByteArray(messageKey), producerId);
        outputStream.write(Constants.OPCODE_WRITE);
        outputStream.write(messageKey);
        outputStream.write(messageValue);
        // outputStream.flush(); not necessary (don't need to block on every write)
        lastSentOffset = storedOffset;
        iterator.next();
      }
      outputStream.flush();
      iterator.close();
      return lastSentOffset;
    }

    // returns committed offset
    private byte[] commit(DataInputStream inputStream, OutputStream outputStream, byte[] offset) throws Exception {
      byte[] opCode = new byte[4];
      byte[] committedOffset = new byte[4];
      LOGGER.info("Requesting commit for offset: {} in Producer: {}", Ints.fromByteArray(offset), producerId);
      outputStream.write(Constants.OPCODE_COMMIT);
      outputStream.write(offset);
      outputStream.flush();

      inputStream.readFully(opCode);
      if (!Arrays.equals(opCode, Constants.OPCODE_COMMIT)) {
        throw new IllegalStateException("Illegal opCode: " + Ints.fromByteArray(opCode) + " in Producer: " + producerId);
      }
      inputStream.readFully(committedOffset);
      LOGGER.info("Received commit acknowledgement for offset: {} in Producer: {}",
          Ints.fromByteArray(offset), producerId);
      Preconditions.checkState(Arrays.equals(offset, committedOffset));
      return committedOffset;
    }
  }
}
