package util;

import com.google.common.primitives.Ints;
import system.JobModel;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;

public class Constants {
  public static class Orchestrator {
    public static final int NUM_PROCESSES = 3;
    public static int TOTAL_RUNTIME_SECONDS = 300;
    public static int MAX_RUNTIME_SECONDS = 30;
    public static int MIN_RUNTIME_SECONDS = 10;
    public static int INTERVAL_BETWEEN_RESTART_SECONDS = 5; // required to allow rocksdb locks to be released
  }

  public static class Task {
    public static final int COMMIT_INTERVAL = 10000;
    public static final int TASK_SLEEP_MS = 1;
  }

  public static class Common {
    public static final Random RANDOM = new Random();
    public static final JobModel JOB_MODEL = new JobModel();

    public static final Options DB_OPTIONS = new Options().setCreateIfMissing(true);
    public static final FlushOptions FLUSH_OPTIONS = new FlushOptions().setWaitForFlush(true);

    public static final String SERVER_HOST = "127.0.0.1";

    public static final int OPCODE_LCO_INT = 0;
    public static final byte[] OPCODE_LCO = Ints.toByteArray(OPCODE_LCO_INT);

    public static final int OPCODE_WRITE_INT = 1;
    public static final byte[] OPCODE_WRITE = Ints.toByteArray(OPCODE_WRITE_INT);

    public static final int OPCODE_COMMIT_INT = 2;
    public static final byte[] OPCODE_COMMIT = Ints.toByteArray(OPCODE_COMMIT_INT);

    public static final int OPCODE_DELETE_INT = 3;
    public static final byte[] OPCODE_DELETE = Ints.toByteArray(OPCODE_DELETE_INT);

    public static final String TASK_STORE_BASE_PATH = "state/stores/task";
    public static final String PRODUCER_STORE_BASE_PATH = "state/stores/producer";
    public static final String REPLICATOR_STORE_BASE_PATH = "state/stores/replicator";
    private static final String COMMITTED_OFFSETS_BASE_PATH = "state/offsets";

    public static Path getTaskOffsetFilePath(int taskId) {
      return Paths.get(COMMITTED_OFFSETS_BASE_PATH + "/task-" + taskId + "/MESSAGE_ID");
    }

    public static Path getProducerOffsetFilePath(int producerId) {
      return Paths.get(COMMITTED_OFFSETS_BASE_PATH + "/producer-" + producerId + "/OFFSET");
    }

    public static Path getReplicatorOffsetFilePath(String replicatorId) {
      return Paths.get(COMMITTED_OFFSETS_BASE_PATH + "/replicator-" + replicatorId + "/OFFSET");
    }
  }
}
