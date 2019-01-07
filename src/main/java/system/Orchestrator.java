package system;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Uninterruptibles;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import util.Constants;
import util.Util;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.StartedProcess;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

// NOTE: If running from IDE, must gradle build first
public class Orchestrator {
  static {
    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
    System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "false");
    System.setProperty(SimpleLogger.SHOW_THREAD_NAME_KEY, "false");
    System.setProperty(SimpleLogger.SHOW_SHORT_LOG_NAME_KEY, "true");
    System.setProperty(SimpleLogger.LEVEL_IN_BRACKETS_KEY, "true");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Orchestrator.class);

  public static void main(String[] args) throws Exception {
    setConfiguration(args);

    for (int i = 0; i < Constants.Orchestrator.NUM_ITERATIONS; i++) {
      LOGGER.info("Starting iteration {}", i);
      simulateFailuresWithoutHostAffinity();
      simulateFailuresWithHostAffinity();
      verifyState();
    }
  }

  private static void setConfiguration(String[] args) {
    OptionParser parser = new OptionParser();
    parser.accepts("execution-id").withOptionalArg().ofType(Integer.class);
    parser.accepts("iterations").withOptionalArg().ofType(Integer.class);
    parser.accepts("total-runtime").withOptionalArg().ofType(Integer.class);
    parser.accepts("max-runtime").withOptionalArg().ofType(Integer.class);
    parser.accepts("min-runtime").withOptionalArg().ofType(Integer.class);
    parser.accepts("interval").withOptionalArg().ofType(Integer.class);

    OptionSet options = parser.parse(args);
    if (options.hasArgument("execution-id")) {
      Constants.Common.EXECUTION_ID = (int) options.valueOf("execution-id");
    }
    if (options.hasArgument("iterations")) {
      Constants.Orchestrator.NUM_ITERATIONS = (int) options.valueOf("iterations");
    }
    if (options.hasArgument("total-runtime")) {
      Constants.Orchestrator.TOTAL_RUNTIME_SECONDS = (int) options.valueOf("total-runtime");
    }
    if (options.hasArgument("max-runtime")) {
      Constants.Orchestrator.MAX_RUNTIME_SECONDS = (int) options.valueOf("max-runtime");
    }
    if (options.hasArgument("min-runtime")) {
      Constants.Orchestrator.MIN_RUNTIME_SECONDS = (int) options.valueOf("min-runtime");
    }
    if (options.hasArgument("interval")) {
      Constants.Orchestrator.INTERVAL_BETWEEN_RESTART_SECONDS = (int) options.valueOf("interval");
    }

    LOGGER.info("Configuration: \nExecution ID: {} \nTotal Runtime: {} \nMax Runtime: {} \nMin Runtime: {} \nInterval: {}",
        Constants.Common.EXECUTION_ID,
        Constants.Orchestrator.TOTAL_RUNTIME_SECONDS, Constants.Orchestrator.MAX_RUNTIME_SECONDS,
        Constants.Orchestrator.MIN_RUNTIME_SECONDS, Constants.Orchestrator.INTERVAL_BETWEEN_RESTART_SECONDS
    );
  }

  private static void simulateFailuresWithHostAffinity() {
    LOGGER.info("Simulating Failures with Host Affinity");
    ExecutorService executorService = Executors.newFixedThreadPool(3);
    long finishTimeMs = System.currentTimeMillis() + Duration.ofSeconds(Constants.Orchestrator.TOTAL_RUNTIME_SECONDS).toMillis() / 2;

    for (int i = 0; i < Constants.Orchestrator.NUM_PROCESSES; i++) {
      final int id = i;
      executorService.submit(() -> {
        try {
          while (!Thread.currentThread().isInterrupted()) {
            try {
              int remainingTimeInSeconds =  Math.max((int) (finishTimeMs - System.currentTimeMillis()) / 1000, 0);
              int runtimeInSeconds = Math.min((Constants.Orchestrator.MIN_RUNTIME_SECONDS +
                  Constants.Common.RANDOM.nextInt(Constants.Orchestrator.MAX_RUNTIME_SECONDS -
                      Constants.Orchestrator.MIN_RUNTIME_SECONDS)), remainingTimeInSeconds) + 1;
              if (runtimeInSeconds <= 0) throw new RuntimeException();
              LOGGER.info("Starting process " + id + " with runtime " + runtimeInSeconds);
              new ProcessExecutor().command((getCmd(id)).split("\\s+")) // args must be provided separately from cmd
                  .redirectOutput(Slf4jStream.of("Container " + id).asInfo())
                  .timeout(runtimeInSeconds, TimeUnit.SECONDS) // random timeout for force kill, must be > 0
                  .destroyOnExit()
                  .execute();
            } catch (TimeoutException te) {
              LOGGER.info("Timeout for process " + id);
            } catch (InterruptedException ie) {
              LOGGER.info("Interrupted for process " + id);
              break;
            } catch (Exception e) {
              LOGGER.error("Unexpected error for process " + id, e);
            }
            Uninterruptibles.sleepUninterruptibly(Constants.Orchestrator.INTERVAL_BETWEEN_RESTART_SECONDS, TimeUnit.SECONDS);
          }
          LOGGER.info("Shutting down launcher thread for process " + id);
        } catch (Exception e) {
          LOGGER.error("Error in launcher thread for process " + id);
        }
      });
    }
    Uninterruptibles.sleepUninterruptibly(Constants.Orchestrator.TOTAL_RUNTIME_SECONDS / 2, TimeUnit.SECONDS);
    LOGGER.info("Shutting down executor service.");
    executorService.shutdownNow();
    Uninterruptibles.sleepUninterruptibly(Constants.Orchestrator.MAX_RUNTIME_SECONDS, TimeUnit.SECONDS); // let running processes die.
    LOGGER.info("Shut down process execution.");
  }

  private static void simulateFailuresWithoutHostAffinity() throws Exception {
    LOGGER.info("Simulating Failures without Host Affinity");
    StartedProcess[] processes = new StartedProcess[Constants.Orchestrator.NUM_PROCESSES];

    for (int id = 0; id < Constants.Orchestrator.NUM_PROCESSES; id++) {
      try {
        LOGGER.info("Starting process " + id);
        startProcess(processes, id);
      } catch (Exception e) {
        LOGGER.error("Unexpected error starting process " + id, e);
      }
    }

    long endTime = System.currentTimeMillis() + Constants.Orchestrator.TOTAL_RUNTIME_SECONDS * 1000 / 2;
    while (!Thread.currentThread().isInterrupted() && System.currentTimeMillis() < endTime) {
      Thread.sleep(Constants.Orchestrator.MIN_RUNTIME_SECONDS * 1000);
      nextTransition(processes);
    }

    waitUntilProcessesStable();
    for (StartedProcess process: processes) {
      process.getProcess().destroy();
      process.getProcess().waitFor();
    }
  }

  private static void nextTransition(StartedProcess[] processes) throws Exception {
    int operation = Constants.Common.RANDOM.nextInt(4);
    waitUntilProcessesStable();
    switch (operation) {
      case 0: { // kill random producer
        int pid = Constants.Common.RANDOM.nextInt(Constants.Orchestrator.NUM_PROCESSES);
        LOGGER.info("Next transition: kill producer {}", pid);
        killProducer(processes, pid);
        break;
      }
      case 1: { // kill random replicator
        int pid = Constants.Common.RANDOM.nextInt(Constants.Orchestrator.NUM_PROCESSES);
        String rid = pid + "" + Constants.Common.RANDOM.nextInt(2);
        LOGGER.info("Next transition: kill replicator {}", rid);
        killReplicator(processes, rid);
        break;
      }
      case 2: { // kill both replicators for a producer
        int pid = Constants.Common.RANDOM.nextInt(Constants.Orchestrator.NUM_PROCESSES);
        String rid0 = pid + "" + 0;
        String rid1 = pid + "" + 1;
        LOGGER.info("Next transition: kill both replicators for pid {}", pid);
        killReplicator(processes, rid0);
        killReplicator(processes, rid1);
        break;
      }
      case 3: { // kill both replicators and bounce producer (simulates Replica then Producer failure)
        int pid = Constants.Common.RANDOM.nextInt(Constants.Orchestrator.NUM_PROCESSES);
        LOGGER.info("Next transition: kill both replicators and bounce producer for pid {}", pid);
        String rid0 = pid + "" + 0;
        String rid1 = pid + "" + 1;
        killReplicator(processes, rid0);
        killReplicator(processes, rid1);
        stopProcess(processes, pid);
        startProcess(processes, pid);
        break;
      }
      default: throw new RuntimeException("Unknown operation " + operation);
    }
  }

  private static void killReplicator(StartedProcess[] processes, String rid) throws Exception {
    LOGGER.debug("Killing replicator: {}", rid);
    stopProcess(processes, getReplicatorPid(rid));
    clearReplicatorState(rid);
    startProcess(processes, getReplicatorPid(rid));
  }

  private static void killProducer(StartedProcess[] processes, int pid) throws Exception {
    LOGGER.debug("Killing producer: {}", pid);
    stopProcess(processes, pid);
    clearProducerState(pid);
    String rid = pid + "" + Constants.Common.RANDOM.nextInt(2); // randomly choose a replicator
    int replicatorPid = getReplicatorPid(rid);
    stopProcess(processes, replicatorPid);
    clearReplicatorState(rid);
    startProcess(processes, pid);
    startProcess(processes, replicatorPid);
  }

  private static int getReplicatorPid(String rid) {
    return Constants.Common.JOB_MODEL.replicators.get(rid).left;
  }

  private static void startProcess(StartedProcess[] processes, int id) throws IOException {
    LOGGER.debug("Starting process: {}", id);
    StartedProcess startedProcess = new ProcessExecutor()
        .command((getCmd(id)).split("\\s+")) // args must be provided separately from cmd
        .redirectOutput(Slf4jStream.of("Container " + id).asInfo())
        .destroyOnExit()
        .start();
    processes[id] = startedProcess;
  }

  private static void stopProcess(StartedProcess[] processes, int id) throws InterruptedException {
    LOGGER.debug("Stopping process: {}", id);
    Process process = processes[id].getProcess();
    process.destroy(); // be nice for now to release rocksdb lock. try destroyForcibly later.
    process.waitFor();
    processes[id] = null;
  }

  // TODO simulate stale offset on replicator
  private static void setReplicatorOffset(String rid, int offset) {
  }

  private static void clearProducerState(int id) throws IOException {
    LOGGER.debug("Clearing state for producer: {}", id);
    String producerStoreDir = Constants.Common.getProducerStoreBasePath() + "/" + id;
    Util.rmrf(producerStoreDir);
  }

  private static void clearReplicatorState(String rid) throws IOException {
    LOGGER.debug("Clearing state for replicator: {}", rid);
    String replicatorStoreDir = Constants.Common.getReplicatorStoreBasePath() + "/" + rid;
    try {
      Util.rmrf(replicatorStoreDir);
      Files.deleteIfExists(Constants.Common.getReplicatorOffsetFilePath(rid));
    } catch (Exception e) {
      LOGGER.warn("Error clearing replicator state. Continuing.", e);
    }
  }

  private static void waitUntilProcessesStable() throws InterruptedException {
    boolean allStable = false;
    long startTime = System.currentTimeMillis();
    while (!allStable) {
      boolean allExist = true;
      for (int p = 0; p < 3; p++) {
        for (int r = 0; r < 2; r++) {
          String rid = p + "" + r;
          if (!Files.exists(Constants.Common.getReplicatorOffsetFilePath(rid))) {
            LOGGER.debug("Offset file does not exist yet for Replicator: {}", rid);
            allExist = false;
          }
        }
      }
      if (!allExist) Thread.sleep(1000);
      allStable = allExist;
    }
    long waitTime = (System.currentTimeMillis() - startTime) / 1000;
    if (waitTime > Constants.Orchestrator.MIN_RUNTIME_SECONDS) {
      LOGGER.warn("Waited {} seconds for all processes to be stable", waitTime);
    }
  }

  public static void verifyState() {
    RocksDB.loadLibrary(); // do this once.
    for (int i = 0; i < 3; i++) {
      try {
        LOGGER.info("Verifying state for task " + i);
        verifyState(i);
      } catch (Exception e) {
        throw new RuntimeException("Error verifying state for task " + i, e);
      }
    }
    LOGGER.info("Verification successful.");
  }

  private static void verifyState(int taskId) throws RocksDBException {
    Options dbOptions = new Options().setCreateIfMissing(true);
    RocksDB taskDb = RocksDB.open(dbOptions, Constants.Common.getTaskStoreBasePath() + "/" + taskId);
    RocksDB replicator0Db = RocksDB.open(dbOptions, Constants.Common.getReplicatorStoreBasePath() + "/" + taskId + "0");
    RocksDB replicator1Db = RocksDB.open(dbOptions, Constants.Common.getReplicatorStoreBasePath() + "/" + taskId + "1");
    RocksIterator taskDbIterator = taskDb.newIterator();
    taskDbIterator.seekToFirst();

    long lastCommittedMessageId = Util.readFile(Constants.Common.getTaskOffsetFilePath(taskId));
    LOGGER.info("Last committed message id: {} for task: {}", lastCommittedMessageId, taskId);
    long maxValidKey = lastCommittedMessageId * taskId + lastCommittedMessageId;

    int verifiedKeys = 0;
    for(; taskDbIterator.isValid(); taskDbIterator.next()) {
      byte[] key = taskDbIterator.key();
      long intKey = Longs.fromByteArray(key);
      if (intKey > maxValidKey) break;

      byte[] r0value = replicator0Db.get(key);
      byte[] r1value = replicator1Db.get(key);
      try {
        Preconditions.checkNotNull(r0value); // every key in task exists in r0
        Preconditions.checkNotNull(r1value); // every key in task exists in r1
        Preconditions.checkState(Arrays.equals(taskDbIterator.value(), r0value)); // task and r0 values are equal
        Preconditions.checkState(Arrays.equals(r0value, r1value)); // r0 and r1 values are equal
      } catch (Exception e) {
        LOGGER.error("Verification error for key: {} for task: {}. Num verified entries: {}", intKey, taskId, verifiedKeys, e);
        throw e;
      }
      verifiedKeys++;
    }
    taskDbIterator.close();
    taskDb.close();
    replicator0Db.close();
    replicator1Db.close();
    LOGGER.info("Verified {} keys for task: {}", verifiedKeys, taskId);
  }

  // NOTE: If running from IDE, must gradle build first
  private static String getCmd(int containerId) {
    return "java -Dfile.encoding=UTF-8 -classpath build/libs/c2c-replication-0.1.jar system.Container " + containerId +
        " --execution-id " + Constants.Common.EXECUTION_ID;
  }
}
