package system;

import com.google.common.primitives.Longs;
import util.Constants;
import util.Util;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);

  private final Integer taskId;
  private final RocksDB taskDb;
  private final Producer producer;
  private final Path commitFilePath;

  private long messageId; // incrementing messageId

  public Task(Integer taskId, RocksDB taskDb, Producer producer) {
    this.taskId = taskId;
    this.taskDb = taskDb;
    this.producer = producer;
    this.commitFilePath = Constants.Common.getTaskOffsetFilePath(taskId);

    // contains the last committed messageId (not offset)
    this.messageId = Util.readFile(commitFilePath) + 1;
    LOGGER.info("Restoring messageId to: {} for Task: {}", messageId, taskId);
  }

  public void run() {
    LOGGER.info("Task {} is now running.", taskId);
    final byte[] key = new byte[8];
    final byte[] value = new byte[8];

    try {
      while(!Thread.currentThread().isInterrupted()) {
        if (Constants.Task.TASK_SLEEP_MS > 0) {
          Thread.sleep(Constants.Task.TASK_SLEEP_MS);
        }

        long data = messageId * taskId + messageId;
        ByteBuffer.wrap(key).putLong(data); // different key pattern per task.
        ByteBuffer.wrap(value).putLong(data);
        taskDb.put(key, value);
        producer.send(key, value);

        // delete older messages to keep state size bounded
        if (messageId > Constants.Task.MAX_NUM_MESSAGES) {
          long oldMessageId = messageId - Constants.Task.MAX_NUM_MESSAGES;
          long oldData = oldMessageId * taskId + oldMessageId;
          byte[] oldKey = Longs.toByteArray(oldData);
          taskDb.delete(oldKey);
          producer.send(oldKey, null);
        }

        if (messageId % Constants.Task.COMMIT_INTERVAL == 0) {
          LOGGER.info("Requesting commit for messageId: {} in Task: {}.", messageId, taskId);
          producer.commit();
          taskDb.flush(Constants.Common.FLUSH_OPTIONS);
          Util.writeFile(commitFilePath, messageId);
        }
        messageId++;
      }
    } catch (Exception e) {
      throw new RuntimeException("Error running task.", e);
    }
  }
}
