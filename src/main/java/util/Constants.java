package util;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import system.JobModel;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;

public class Constants {
  public static class Orchestrator {
    public static final int NUM_PROCESSES = 3;
    public static int NUM_ITERATIONS = 1;
    public static int TOTAL_RUNTIME_SECONDS = 600;
    public static int MAX_RUNTIME_SECONDS = 30;
    public static int MIN_RUNTIME_SECONDS = 20;
    public static int INTERVAL_BETWEEN_RESTART_SECONDS = 5; // required to allow rocksdb locks to be released
  }

  public static class Task {
    public static final int COMMIT_INTERVAL = 10000;
    public static final int TASK_SLEEP_MS = 0;
    public static final int MAX_NUM_MESSAGES = 100000;
  }

  public static class Common {
    public static int EXECUTION_ID = 0;

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

    public static final byte[] DELETE_PAYLOAD = Longs.toByteArray(Integer.MIN_VALUE);

    private static final String STATE_BASE_PATH = "state";
    private static final String TASK_STORE_BASE_PATH = "stores/task";
    private static final String PRODUCER_STORE_BASE_PATH = "stores/producer";
    private static final String REPLICATOR_STORE_BASE_PATH = "stores/replicator";
    private static final String COMMITTED_OFFSETS_BASE_PATH = "offsets";
    private static final String REPLICATOR_PORTS_BASE_PATH = "ports";

    public static String getTaskStoreBasePath() {
      return STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + TASK_STORE_BASE_PATH;
    }

    public static String getProducerStoreBasePath() {
      return STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + PRODUCER_STORE_BASE_PATH;
    }

    public static String getReplicatorStoreBasePath() {
      return STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + REPLICATOR_STORE_BASE_PATH;
    }

    public static Path getTaskOffsetFilePath(int taskId) {
      return Paths.get(STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + COMMITTED_OFFSETS_BASE_PATH + "/task/" + taskId + "/MESSAGE_ID");
    }

    public static Path getProducerOffsetFilePath(int producerId) {
      return Paths.get(STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + COMMITTED_OFFSETS_BASE_PATH + "/producer/" + producerId + "/OFFSET");
    }

    public static Path getReplicatorOffsetFilePath(String replicatorId) {
      return Paths.get(STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + COMMITTED_OFFSETS_BASE_PATH + "/replicator/" + replicatorId + "/OFFSET");
    }

    public static Path getReplicatorPortPath(String replicatorId) {
      return Paths.get(STATE_BASE_PATH + "/" + EXECUTION_ID + "/" + REPLICATOR_PORTS_BASE_PATH + "/replicator/" + replicatorId + "/PORT");
    }
  }
}
