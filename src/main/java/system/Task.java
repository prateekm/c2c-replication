package system;

import com.google.common.primitives.Ints;
import util.Constants;
import util.Util;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);
  private static final FlushOptions FLUSH_OPTIONS = new FlushOptions().setWaitForFlush(true);
  private static final int COMMIT_INTERVAL = 100000;

  private final Integer taskId;
  private final RocksDB taskDb;
  private final Producer producer;
  private final Path commitFilePath;

  private int messageId; // incrementing messageId

  public Task(Integer taskId, RocksDB taskDb, Producer producer) {
    this.taskId = taskId;
    this.taskDb = taskDb;
    this.producer = producer;
    this.commitFilePath = Constants.getTaskOffsetFile(taskId);

    // contains the last committed messageId (not offset)
    this.messageId = Ints.fromByteArray(Util.readFile(commitFilePath));
  }

  public void run() {
    LOGGER.info("Task {} is now running.", taskId);
    final byte[] key = new byte[4];
    final byte[] value = new byte[4];

    try {
      while(!Thread.currentThread().isInterrupted()) {
//        Thread.sleep(Constants.RANDOM.nextInt(10));

        int data = messageId * taskId + messageId;
        ByteBuffer.wrap(key).putInt(data); // different key pattern per task.
        ByteBuffer.wrap(value).putInt(data);
        taskDb.put(key, value);

        producer.send(key, value);
        if (messageId % COMMIT_INTERVAL == 0) {
          LOGGER.info("Requesting Producer commit for messageId: {} in Task: {}.", messageId, taskId);
          producer.commit();
          taskDb.flush(FLUSH_OPTIONS);
          Util.writeFile(commitFilePath, Ints.toByteArray(messageId));
        }
        messageId++;
      }
    } catch (Exception e) {
      throw new RuntimeException("Error running task.", e);
    }
  }
}
