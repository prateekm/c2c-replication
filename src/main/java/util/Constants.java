package util;

import com.google.common.primitives.Ints;
import system.JobModel;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class Constants {
  public static final Random RANDOM = new Random();
  public static final JobModel JOB_MODEL = new JobModel();

  public static final String SERVER_HOST = "127.0.0.1";

  public static final int OPCODE_SYNC_INT = 0;
  public static final byte[] OPCODE_SYNC = Ints.toByteArray(OPCODE_SYNC_INT);
  public static final int OPCODE_WRITE_INT = 1;
  public static final byte[] OPCODE_WRITE = Ints.toByteArray(OPCODE_WRITE_INT);
  public static final int OPCODE_COMMIT_INT = 2;
  public static final byte[] OPCODE_COMMIT = Ints.toByteArray(OPCODE_COMMIT_INT);

  public static final String TASK_STORE_BASE_PATH = "state/stores/task";
  public static final String PRODUCER_STORE_BASE_PATH = "state/stores/producer";
  public static final String REPLICATOR_STORE_BASE_PATH = "state/stores/replicator";

  private static final String COMMITTED_OFFSETS_BASE_PATH = "state/offsets";
  public static Path getTaskOffsetFile(int taskId) {
    return Paths.get(Constants.COMMITTED_OFFSETS_BASE_PATH + "/task-" + taskId + "/MESSAGE_ID");
  }

  public static Path getReplicatorOffsetFile(String replicatorId) {
    return Paths.get(Constants.COMMITTED_OFFSETS_BASE_PATH + "/replicator-" + replicatorId + "/OFFSET");
  }
}
