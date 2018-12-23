package system;

import com.google.common.primitives.Ints;
import util.Constants;
import util.Util;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Replicator extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);
  private final String replicatorId;
  private final Integer replicatorPort;
  private final RocksDB replicatorDb;
  private final Path offsetFilePath;

  public Replicator(String replicatorId, Integer replicatorPort, RocksDB replicatorDb, String replicatorDbBasePath) {
    this.replicatorId = replicatorId;
    this.replicatorPort = replicatorPort;
    this.replicatorDb = replicatorDb;
    this.offsetFilePath = Paths.get(replicatorDbBasePath + "/" + replicatorId + "/OFFSET");
  }

  public void run() {
    LOGGER.info("Replicator {} is now running.", replicatorId);
    Processor processor = null;
    try (ServerSocket serverSocket = new ServerSocket(replicatorPort)) {
      while (!Thread.currentThread().isInterrupted()) {
        Socket socket = serverSocket.accept();
        processor = new Processor(replicatorId, socket, replicatorDb, offsetFilePath);
        processor.start();
      }
      LOGGER.error("Exiting connection accept loop in Replicator " + replicatorId);
    } catch (Exception e) {
      LOGGER.error("Error creating socket connection in Replicator " + replicatorId, e);
    } finally {
      try {
        if (processor != null) {
          processor.join(1000);
        }
      } catch (Exception e) {
        LOGGER.error("Timeout shutting down processor in Replicator " + replicatorId, e);
      }
    }
  }

  private static class Processor extends Thread {
    private final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true);
    private final String replicatorId;
    private final Socket socket;
    private final RocksDB db;
    private final Path offsetFilePath;

    Processor(String replicatorId, Socket socket, RocksDB db, Path offsetFilePath) {
      super();
      this.replicatorId = replicatorId;
      this.socket = socket;
      this.db = db;
      this.offsetFilePath = offsetFilePath;
    }

    @Override
    public void run() {
      try {
        DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        OutputStream outputStream = socket.getOutputStream();

        final byte[] opCode = new byte[4];

        while (!Thread.currentThread().isInterrupted()) {
          inputStream.readFully(opCode);

          switch (Ints.fromByteArray(opCode)) {
            case Constants.OPCODE_SYNC_INT:
              handleSync(outputStream);
              break;
            case Constants.OPCODE_WRITE_INT:
              handleWrite(inputStream);
              break;
            case Constants.OPCODE_COMMIT_INT:
              handleCommit(inputStream, outputStream);
              break;
            default:
              throw new UnsupportedOperationException("Unknown opCode: " + Ints.fromByteArray(opCode) + " in Replicator " + replicatorId);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Processor Error. Shutting down thread in Replicator " + replicatorId, e);
      } finally {
        try {
          socket.close();
        } catch (Exception e) {
          LOGGER.error("Socket close error during processor shutdown in Replicator " + replicatorId, e);
        }
      }
    }

    private void handleSync(OutputStream outputStream) throws IOException {
      ByteBuffer response = ByteBuffer.allocate(4 + 4);
      LOGGER.info("Received sync request in Replicator " + replicatorId);
      byte[] offset = Util.readFile(offsetFilePath);
      LOGGER.info("Requesting sync from offset " + Ints.fromByteArray(offset) + " in Replicator " + replicatorId);
      response.putInt(Constants.OPCODE_SYNC_INT);
      response.put(offset);
      response.flip();
      outputStream.write(response.array());
      outputStream.flush();
      response.clear();
    }

    private void handleWrite(DataInputStream inputStream) throws IOException, RocksDBException {
      final byte[] key = new byte[4];
      final byte[] value = new byte[4];

      LOGGER.debug("Received write request for key {} in Replicator {}", Ints.fromByteArray(key), replicatorId);
      inputStream.readFully(key);
      inputStream.readFully(value);
      db.put(key, value);
    }

    private void handleCommit(DataInputStream inputStream, OutputStream outputStream) throws Exception {
      final byte[] offset = new byte[4];

      inputStream.readFully(offset);
      LOGGER.info("Received commit request for offset {}", Ints.fromByteArray(offset) + " in Replicator " + replicatorId);

      db.flush(flushOptions);
      Util.writeFile(offsetFilePath, offset);
      LOGGER.info("Acknowledging commit request for offset {}  in Replicator " + replicatorId, Ints.fromByteArray(offset));
      ByteBuffer response = ByteBuffer.allocate(4 + 4);
      response.putInt(Constants.OPCODE_COMMIT_INT);
      response.put(offset);
      response.flip();
      outputStream.write(response.array());
      outputStream.flush();
      response.clear();
    }
  }
}
