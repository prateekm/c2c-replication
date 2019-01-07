package system;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import util.Constants;
import util.Util;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Replicator extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);
  private final String replicatorId;

  public Replicator(String replicatorId) {
    this.replicatorId = replicatorId;
  }

  public void run() {
    LOGGER.info("Replicator: {} is now running.", replicatorId);
    ConnectionHandler connectionHandler = null;
    try (ServerSocket serverSocket = new ServerSocket()) {
      serverSocket.bind(null);
      int replicatorPort = serverSocket.getLocalPort();
      Util.writeFile(Constants.Common.getReplicatorPortPath(replicatorId), replicatorPort);

      while (!Thread.currentThread().isInterrupted()) {
        Socket socket = serverSocket.accept();
        connectionHandler = new ConnectionHandler(replicatorId, socket);
        connectionHandler.start();
      }
      LOGGER.info("Exiting connection accept loop in Replicator: {}", replicatorId);
    } catch (Exception e) {
      throw new RuntimeException("Error handling connection in Replicator." + replicatorId, e);
    }
  }

  private static class ConnectionHandler extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionHandler.class);

    private final String replicatorId;
    private final Socket socket;
    private final Path offsetFilePath;

    private RocksDB replicatorDb;

    ConnectionHandler(String replicatorId, Socket socket) throws Exception {
      super("ConnectionHandler " + replicatorId);
      this.replicatorId = replicatorId;
      this.socket = socket;
      this.replicatorDb = createReplicatorDb(replicatorId);
      this.offsetFilePath = Constants.Common.getReplicatorOffsetFilePath(replicatorId);
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
            case Constants.Common.OPCODE_LCO_INT:
              handleLCO(outputStream);
              break;
            case Constants.Common.OPCODE_WRITE_INT:
              handleWrite(inputStream);
              break;
            case Constants.Common.OPCODE_COMMIT_INT:
              handleCommit(inputStream, outputStream);
              break;
            case Constants.Common.OPCODE_DELETE_INT:
              handleDelete(outputStream);
              break;
            default:
              throw new UnsupportedOperationException("Unknown opCode: " + Ints.fromByteArray(opCode) + " in Replicator: " + replicatorId);
          }
        }
      } catch (EOFException | SocketException e) {
        LOGGER.info("Shutting down connection handler in Replicator: {}", replicatorId);
      } catch (Exception e) {
        LOGGER.info("Shutting down connection handler in Replicator: {}", replicatorId, e);
      } finally {
        try {
          socket.close();
          if (replicatorDb.isOwningHandle()) replicatorDb.close();
        } catch (Exception e) {
          LOGGER.info("Error during ConnectionHandler shutdown in Replicator: {}", replicatorId, e);
        }
      }
    }

    private void handleLCO(OutputStream outputStream) throws IOException {
      ByteBuffer response = ByteBuffer.allocate(4 + 8);
      LOGGER.debug("Received sync request in Replicator: {}", replicatorId);
      long offset = Util.readFile(offsetFilePath);
      LOGGER.debug("Requesting sync from offset: {} in Replicator: {}", offset, replicatorId);
      response.putInt(Constants.Common.OPCODE_LCO_INT);
      response.putLong(offset);
      response.flip();
      outputStream.write(response.array());
      outputStream.flush();
    }

    private void handleWrite(DataInputStream inputStream) throws IOException, RocksDBException {
      final byte[] key = new byte[8];
      final byte[] value = new byte[8];

      LOGGER.debug("Received write request for key: {} in Replicator: {}", Longs.fromByteArray(key), replicatorId);
      inputStream.readFully(key);
      inputStream.readFully(value);
      if (Arrays.equals(value, Constants.Common.DELETE_PAYLOAD)) {
        replicatorDb.delete(key);
      } else {
        replicatorDb.put(key, value);
      }
    }

    private void handleCommit(DataInputStream inputStream, OutputStream outputStream) throws Exception {
      final byte[] offset = new byte[8];

      inputStream.readFully(offset);
      LOGGER.info("Received commit request for offset: {} in Replicator: {}", Longs.fromByteArray(offset), replicatorId);

      replicatorDb.flush(Constants.Common.FLUSH_OPTIONS);
      Util.writeFile(offsetFilePath, Longs.fromByteArray(offset));
      LOGGER.info("Acknowledging commit request for offset: {} in Replicator: {}", Longs.fromByteArray(offset), replicatorId);
      ByteBuffer response = ByteBuffer.allocate(4 + 8);
      response.putInt(Constants.Common.OPCODE_COMMIT_INT);
      response.put(offset);
      response.flip();
      outputStream.write(response.array());
      outputStream.flush();
    }

    private void handleDelete(OutputStream outputStream) throws Exception {
      LOGGER.info("Received delete request in Replicator: {}", replicatorId);

      if (replicatorDb.isOwningHandle()) replicatorDb.close();
      try {
        Files.deleteIfExists(Constants.Common.getReplicatorOffsetFilePath(replicatorId));
        Util.rmrf(Constants.Common.getReplicatorStoreBasePath() + "/" + replicatorId);
      } catch (Exception e) {
        LOGGER.warn("Error handling replicator delete. Continuing.", e);
      }

      this.replicatorDb = createReplicatorDb(replicatorId);

      LOGGER.info("Acknowledging delete request in Replicator: {}", replicatorId);
      ByteBuffer response = ByteBuffer.allocate(4);
      response.putInt(Constants.Common.OPCODE_DELETE_INT);
      response.flip();
      outputStream.write(response.array());
      outputStream.flush();
    }

    private static RocksDB createReplicatorDb(String replicatorId) throws Exception {
      return RocksDB.open(Constants.Common.DB_OPTIONS, Constants.Common.getReplicatorStoreBasePath() + "/" + replicatorId);
    }
  }
}
