package system;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Uninterruptibles;
import util.Constants;
import util.Util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
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

// TODO NOTE: MUST REBUILD PROJECT BEFORE RUNNING. MUST UPDATE CMD AFTER CHANGING DEPS
public class Orchestrator {
  static {
    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
    System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "false");
    System.setProperty(SimpleLogger.SHOW_THREAD_NAME_KEY, "false");
    System.setProperty(SimpleLogger.SHOW_SHORT_LOG_NAME_KEY, "true");
    System.setProperty(SimpleLogger.LEVEL_IN_BRACKETS_KEY, "true");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Orchestrator.class);
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(3);
  private static final int NUM_PROCESSES = 3;
  private static final long TOTAL_RUNTIME_SECONDS = 300;
  private static final int MAX_RUNTIME_SECONDS = 10;
  private static final int MIN_RUNTIME_SECONDS = 10;
  private static final int MIN_INTERVAL_BETWEEN_START_SECONDS = 5;

  public static void main(String[] args) throws Exception {
    failuresWithoutHostAffinity();
    verifyState();
  }

  private static void failuresWithHostAffinity() {
    long finishTimeMs = System.currentTimeMillis() + Duration.ofSeconds(TOTAL_RUNTIME_SECONDS).toMillis();

    for (int i = 0; i < NUM_PROCESSES; i++) {
      final int id = i;
      EXECUTOR_SERVICE.submit(() -> {
        try {
          while (!Thread.currentThread().isInterrupted()) {
            try {
              int remainingTimeInSeconds =  Math.max((int) (finishTimeMs - System.currentTimeMillis()) / 1000, 0);
              int runtimeInSeconds = Math.min((MIN_RUNTIME_SECONDS + Constants.RANDOM.nextInt(MAX_RUNTIME_SECONDS - MIN_RUNTIME_SECONDS)), remainingTimeInSeconds) + 1;
              if (runtimeInSeconds <= 0) throw new RuntimeException();
              LOGGER.info("Starting process " + id + " with runtime " + runtimeInSeconds);
              new ProcessExecutor().command((CMD + " " + id).split("\\s+")) // args must be provided separately from cmd
                  .redirectOutput(Slf4jStream.of("Container " + id).asInfo())
                  .timeout(runtimeInSeconds, TimeUnit.SECONDS) // random timeout for force kill, must be > 0
                  .destroyOnExit()
                  .execute();
            } catch (TimeoutException te) {
              LOGGER.error("Timeout for process " + id);
            } catch (InterruptedException ie) {
              LOGGER.error("Interrupted for process " + id);
              break;
            } catch (Exception e) {
              LOGGER.error("Unexpected error for process " + id, e);
            }
            Uninterruptibles.sleepUninterruptibly(MIN_INTERVAL_BETWEEN_START_SECONDS, TimeUnit.SECONDS);
          }
          LOGGER.info("Shutting down launcher thread for process " + id);
        } catch (Exception e) {
          LOGGER.error("Error in launcher thread for process " + id);
        }
      });
    }
    Uninterruptibles.sleepUninterruptibly(TOTAL_RUNTIME_SECONDS, TimeUnit.SECONDS);
    LOGGER.info("Shutting down executor service.");
    EXECUTOR_SERVICE.shutdownNow();
    Uninterruptibles.sleepUninterruptibly(MAX_RUNTIME_SECONDS, TimeUnit.SECONDS); // let running processes die.
    LOGGER.info("Shut down process execution.");
  }

  private static void failuresWithoutHostAffinity() throws Exception {
    StartedProcess[] processes = new StartedProcess[NUM_PROCESSES];

    for (int id = 0; id < NUM_PROCESSES; id++) {
      try {
        LOGGER.info("Starting process " + id);
        startProcess(processes, id);
      } catch (Exception e) {
        LOGGER.error("Unexpected error starting process " + id, e);
      }
    }

    long endTime = System.currentTimeMillis() + TOTAL_RUNTIME_SECONDS * 1000;
    while (!Thread.currentThread().isInterrupted() && System.currentTimeMillis() < endTime) {
      Thread.sleep(MIN_RUNTIME_SECONDS * 1000);
      nextTransition(processes);
    }

    waitUntilProcessesStable();
    for (StartedProcess process: processes) {
      process.getProcess().destroy();
      process.getProcess().waitFor();
    }
  }

  private static void nextTransition(StartedProcess[] processes) throws Exception {
    int operation = Constants.RANDOM.nextInt(4);
    waitUntilProcessesStable();
    switch (operation) {
      case 0: { // kill random producer
        int pid = Constants.RANDOM.nextInt(NUM_PROCESSES);
        LOGGER.info("Next operation: kill producer {}", pid);
        killProducer(processes, pid);
        break;
      }
      case 1: { // kill random replicator
        int pid = Constants.RANDOM.nextInt(NUM_PROCESSES);
        String rid = pid + "" + Constants.RANDOM.nextInt(2);
        LOGGER.info("Next operation: kill replicator {}", rid);
        killReplicator(processes, rid);
        break;
      }
      case 2: { // kill both replicators for a producer
        int pid = Constants.RANDOM.nextInt(NUM_PROCESSES);
        String rid0 = pid + "" + 0;
        String rid1 = pid + "" + 1;
        LOGGER.info("Next operation: kill both replicators for pid {}", pid);
        killReplicator(processes, rid0);
        killReplicator(processes, rid1);
        break;
      }
      case 3: { // kill both replicators and bounce producer (simulates Replica then Producer failure)
        int pid = Constants.RANDOM.nextInt(NUM_PROCESSES);
        LOGGER.info("Next operation: kill both replicators and bounce producer for pid {}", pid);
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
    LOGGER.info("Killing replicator: {}", rid);
    stopProcess(processes, getReplicatorPid(rid));
    clearReplicatorState(rid);
    startProcess(processes, getReplicatorPid(rid));
  }

  private static void killProducer(StartedProcess[] processes, int pid) throws Exception {
    LOGGER.info("Killing producer: {}", pid);
    stopProcess(processes, pid);
    clearProducerState(pid);
    String rid = pid + "" + Constants.RANDOM.nextInt(2); // randomly choose a replicator
    int replicatorPid = getReplicatorPid(rid);
    stopProcess(processes, replicatorPid);
    clearReplicatorState(rid);
    startProcess(processes, pid);
    startProcess(processes, replicatorPid);
  }

  private static int getReplicatorPid(String rid) {
    return Constants.JOB_MODEL.replicators.get(rid).left;
  }

  private static void startProcess(StartedProcess[] processes, int id) throws IOException {
    LOGGER.info("Starting process: {}", id);
    StartedProcess startedProcess = new ProcessExecutor()
        .command((CMD + " " + id).split("\\s+")) // args must be provided separately from cmd
        .redirectOutput(Slf4jStream.of("Container " + id).asInfo())
        .destroyOnExit()
        .start();
    processes[id] = startedProcess;
  }

  private static void stopProcess(StartedProcess[] processes, int id) throws InterruptedException {
    LOGGER.info("Stopping process: {}", id);
    Process process = processes[id].getProcess();
    process.destroy(); // TODO be nice for now to release rocksdb locks. try destroyForcibly later.
    process.waitFor();
    processes[id] = null;
  }

  // TODO simulate stale offset on replicator
  private static void setReplicatorOffset(String rid, int offset) {
  }

  private static void clearProducerState(int id) throws IOException {
    LOGGER.info("Clearing state for producer: {}", id);
    String producerStoreDir = Constants.PRODUCER_STORE_BASE_PATH + "/" + id;
    Util.rmrf(producerStoreDir);
  }

  private static void clearReplicatorState(String rid) throws IOException {
    LOGGER.info("Clearing state for replicator: {}", rid);
    String replicatorStoreDir = Constants.REPLICATOR_STORE_BASE_PATH + "/" + rid;
    try {
      Util.rmrf(replicatorStoreDir);
      Files.deleteIfExists(Constants.getReplicatorOffsetFile(rid));
    } catch (Exception e) {
      LOGGER.warn("Error clearing replicator state. Continuing.", e);
    }
  }

  private static void waitUntilProcessesStable() throws InterruptedException {
    boolean allStable = false;
    long startTime = System.currentTimeMillis();
    while (!allStable) {
      Thread.sleep(1000);
      boolean allExist = true;
      for (int p = 0; p < 3; p++) {
        for (int r = 0; r < 2; r++) {
          String rid = p + "" + r;
          if (!Files.exists(Constants.getReplicatorOffsetFile(rid))) {
            LOGGER.debug("Offset file does not exist yet for Replicator: {}", rid);
            allExist = false;
          }
        }
      }
      allStable = allExist;
    }
    LOGGER.info("Waited {} seconds for all processes to be stable", (System.currentTimeMillis() - startTime) / 1000);
  }

  private static void verifyState() {
    RocksDB.loadLibrary(); // do this once.
    for (int i = 0; i < 3; i++) {
      try {
        LOGGER.info("Verifying state for task " + i);
        verifyState(i);
      } catch (Exception e) {
        LOGGER.error("Error verifying state for task " + i, e);
      }
    }
  }

  private static void verifyState(int taskId) throws RocksDBException {
    Options dbOptions = new Options().setCreateIfMissing(true);
    RocksDB taskDb = RocksDB.open(dbOptions, Constants.TASK_STORE_BASE_PATH + "/" + taskId);
    RocksDB replicator0Db = RocksDB.open(dbOptions, Constants.REPLICATOR_STORE_BASE_PATH + "/" + taskId + "0");
    RocksDB replicator1Db = RocksDB.open(dbOptions, Constants.REPLICATOR_STORE_BASE_PATH + "/" + taskId + "1");
    RocksIterator taskDbIterator = taskDb.newIterator();
    taskDbIterator.seekToFirst();

    byte[] lastCommittedMessageId = Util.readFile(Constants.getTaskOffsetFile(taskId));
    int messageId = Ints.fromByteArray(lastCommittedMessageId);
    LOGGER.info("Last committed message id: {} for task: {}", messageId, taskId);
    int maxValidKey = messageId * taskId + messageId;

    int verifiedKeys = 0;
    for(; taskDbIterator.isValid(); taskDbIterator.next()) {
      byte[] key = taskDbIterator.key();
      int intKey = Ints.fromByteArray(key);
      if (intKey > maxValidKey) break;

      byte[] r0value = replicator0Db.get(key);
      byte[] r1value = replicator1Db.get(key);
      try {
        Preconditions.checkNotNull(r0value); // every key in task exists in r0
        Preconditions.checkNotNull(r1value); // every key in task exists in r1
        Preconditions.checkState(Arrays.equals(taskDbIterator.value(), r0value)); // task and r0 values are equal
        Preconditions.checkState(Arrays.equals(r0value, r1value)); // r0 and r1 values are equal
      } catch (Exception e) {
        LOGGER.error("Verification error for key: {} for task: {}", intKey, taskId, e);
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

  // TODO NOTE: MUST REBUILD PROJECT BEFORE RUNNING. MUST UPDATE CMD AFTER CHANGING DEPS
  private static final String CMD = "/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/bin/java -Dfile.encoding=UTF-8 -classpath /Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/charsets.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/deploy.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/cldrdata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/dnsns.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/jaccess.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/jfxrt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/localedata.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/nashorn.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/sunec.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/sunjce_provider.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/sunpkcs11.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/ext/zipfs.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/javaws.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/jce.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/jfr.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/jfxswt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/jsse.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/management-agent.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/plugin.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/resources.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/jre/lib/rt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/ant-javafx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/dt.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/javafx-mx.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/jconsole.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/packager.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/sa-jdi.jar:/Library/Java/JavaVirtualMachines/jdk1.8.0_172.jdk/Contents/Home/lib/tools.jar:/Users/pmaheshw/code/personal/c2c-replication/out/production/networking:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.guava/guava/27.0.1-jre/bd41a290787b5301e63929676d792c507bbc00ae/guava-27.0.1-jre.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/io.dropwizard.metrics/metrics-jmx/4.0.3/d49313634a606496433e34b733251ba9fdbb333f/metrics-jmx-4.0.3.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/io.prometheus/simpleclient_dropwizard/0.6.0/eb5bf2734c90f56350e9d4e27ea93486b4c5b9e5/simpleclient_dropwizard-0.6.0.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/io.dropwizard.metrics/metrics-core/4.0.3/bb562ee73f740bb6b2bf7955f97be6b870d9e9f0/metrics-core-4.0.3.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/io.prometheus/simpleclient_hotspot/0.6.0/2703b02c4b2abb078de8365f4ef3b7d5e451382d/simpleclient_hotspot-0.6.0.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/io.prometheus/simpleclient/0.6.0/26073e94cbfa6780e10ef524e542cf2a64dabe67/simpleclient-0.6.0.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.rocksdb/rocksdbjni/5.5.1/57c2e04b9d2e572f6684d23fb3f33a84a289a405/rocksdbjni-5.5.1.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-simple/1.7.2/760055906d7353ba4f7ce1b8908bc6b2e91f39fa/slf4j-simple-1.7.2.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.zeroturnaround/zt-exec/1.10/6bec7a4af16208c7542d2c280207871c7b976483/zt-exec-1.10.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-api/1.7.25/da76ca59f6a57ee3102f8f9bd9cee742973efa8a/slf4j-api-1.7.25.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.guava/failureaccess/1.0.1/1dcf1de382a0bf95a3d8b0849546c88bac1292c9/failureaccess-1.0.1.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/b421526c5f297295adef1c886e5246c39d4ac629/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.code.findbugs/jsr305/3.0.2/25ea2e8b0c338a877313bd4672d3fe056ea78f0d/jsr305-3.0.2.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.checkerframework/checker-qual/2.5.2/cea74543d5904a30861a61b4643a5f2bb372efc4/checker-qual-2.5.2.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.errorprone/error_prone_annotations/2.2.0/88e3c593e9b3586e1c6177f89267da6fc6986f0c/error_prone_annotations-2.2.0.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/com.google.j2objc/j2objc-annotations/1.1/ed28ded51a8b1c6b112568def5f4b455e6809019/j2objc-annotations-1.1.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/org.codehaus.mojo/animal-sniffer-annotations/1.17/f97ce6decaea32b36101e37979f8b647f00681fb/animal-sniffer-annotations-1.17.jar:/Users/pmaheshw/.gradle/caches/modules-2/files-2.1/commons-io/commons-io/1.4/a8762d07e76cfde2395257a5da47ba7c1dbd3dce/commons-io-1.4.jar system.Container";

}
