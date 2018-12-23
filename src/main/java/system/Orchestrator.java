package system;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Uninterruptibles;
import util.Constants;
import util.Util;

import java.nio.file.Paths;
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
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

// TODO NOTE: MUST REBUILD PROJECT BEFORE RUNNING. MUST UPDATE cmd AFTER CHANGING DEPS
public class Orchestrator {
  private static final Logger LOGGER = LoggerFactory.getLogger(Orchestrator.class);
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(3);
  private static final long TOTAL_RUNTIME_SECONDS = 1200;
  private static final int MAX_RUNTIME_SECONDS = 120;
  private static final int MIN_RUNTIME_SECONDS = 100;
  private static final int MIN_INTERVAL_BETWEEN_START_SECONDS = 2;

  // TODO NOTE: MUST REBUILD PROJECT BEFORE RUNNING. MUST UPDATE cmd AFTER CHANGING DEPS
  public static void main(String[] args) {
    execute();
    verifyState();
  }

  private static void execute() {
    long finishTimeMs = System.currentTimeMillis() + Duration.ofSeconds(TOTAL_RUNTIME_SECONDS).toMillis();

    for (int i = 0; i < 3; i++) {
      final int id = i;
      EXECUTOR_SERVICE.submit(() -> {
        try {
          while (!Thread.currentThread().isInterrupted()) {
            try {
              String cmd = "/Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home/bin/java -Dfile.encoding=UTF-8 -classpath /Users/prateekm/code/personal/c2c-replication/out/production/classes:/Users/prateekm/.gradle/caches/modules-2/files-2.1/com.google.guava/guava/27.0.1-jre/bd41a290787b5301e63929676d792c507bbc00ae/guava-27.0.1-jre.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/io.dropwizard.metrics/metrics-jmx/4.0.3/d49313634a606496433e34b733251ba9fdbb333f/metrics-jmx-4.0.3.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/io.prometheus/simpleclient_dropwizard/0.6.0/eb5bf2734c90f56350e9d4e27ea93486b4c5b9e5/simpleclient_dropwizard-0.6.0.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/io.dropwizard.metrics/metrics-core/4.0.3/bb562ee73f740bb6b2bf7955f97be6b870d9e9f0/metrics-core-4.0.3.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/io.prometheus/simpleclient_hotspot/0.6.0/2703b02c4b2abb078de8365f4ef3b7d5e451382d/simpleclient_hotspot-0.6.0.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/io.prometheus/simpleclient/0.6.0/26073e94cbfa6780e10ef524e542cf2a64dabe67/simpleclient-0.6.0.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/org.rocksdb/rocksdbjni/5.5.1/57c2e04b9d2e572f6684d23fb3f33a84a289a405/rocksdbjni-5.5.1.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-simple/1.7.2/760055906d7353ba4f7ce1b8908bc6b2e91f39fa/slf4j-simple-1.7.2.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/org.zeroturnaround/zt-exec/1.10/6bec7a4af16208c7542d2c280207871c7b976483/zt-exec-1.10.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/org.slf4j/slf4j-api/1.7.25/da76ca59f6a57ee3102f8f9bd9cee742973efa8a/slf4j-api-1.7.25.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/com.google.guava/failureaccess/1.0.1/1dcf1de382a0bf95a3d8b0849546c88bac1292c9/failureaccess-1.0.1.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/com.google.guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/b421526c5f297295adef1c886e5246c39d4ac629/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/com.google.code.findbugs/jsr305/3.0.2/25ea2e8b0c338a877313bd4672d3fe056ea78f0d/jsr305-3.0.2.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/org.checkerframework/checker-qual/2.5.2/cea74543d5904a30861a61b4643a5f2bb372efc4/checker-qual-2.5.2.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/com.google.errorprone/error_prone_annotations/2.2.0/88e3c593e9b3586e1c6177f89267da6fc6986f0c/error_prone_annotations-2.2.0.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/com.google.j2objc/j2objc-annotations/1.1/ed28ded51a8b1c6b112568def5f4b455e6809019/j2objc-annotations-1.1.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/org.codehaus.mojo/animal-sniffer-annotations/1.17/f97ce6decaea32b36101e37979f8b647f00681fb/animal-sniffer-annotations-1.17.jar:/Users/prateekm/.gradle/caches/modules-2/files-2.1/commons-io/commons-io/1.4/a8762d07e76cfde2395257a5da47ba7c1dbd3dce/commons-io-1.4.jar system.Container " + id;
              int remainingTimeInSeconds =  Math.max((int) (finishTimeMs - System.currentTimeMillis()) / 1000, 0);
              int runtimeInSeconds = Math.min((MIN_RUNTIME_SECONDS + Constants.RANDOM.nextInt(MAX_RUNTIME_SECONDS - MIN_RUNTIME_SECONDS)), remainingTimeInSeconds) + 1;
              if (runtimeInSeconds <= 0) throw new RuntimeException();
              LOGGER.info("Starting process " + id + " with runtime " + runtimeInSeconds);
              new ProcessExecutor().command(cmd.split("\\s+")) // args must be provided separately from cmd
                  .redirectOutput(Slf4jStream.of("Container " + id).asInfo())
                  .timeout(runtimeInSeconds, TimeUnit.SECONDS) // random timeout for force kill, must be > 0
                  .destroyOnExit()
                  .execute();
            } catch (TimeoutException te) {
              LOGGER.error("Timeout for process " + id, te);
            } catch (InterruptedException ie) {
              LOGGER.error("Interrupted for process " + id, ie);
              break;
            } catch (Exception e) {
              LOGGER.error("Unexpected error for process " + id, e);
            }
            Uninterruptibles.sleepUninterruptibly(MIN_INTERVAL_BETWEEN_START_SECONDS, TimeUnit.SECONDS);
          }
          LOGGER.info("Shutting down launcher thread for id " + id);
        } catch (Exception e) {
          LOGGER.error("Error in launcher thread for id " + id);
        }
      });
    }
    Uninterruptibles.sleepUninterruptibly(TOTAL_RUNTIME_SECONDS, TimeUnit.SECONDS);
    LOGGER.info("Shutting down executor service.");
    EXECUTOR_SERVICE.shutdownNow();
    Uninterruptibles.sleepUninterruptibly(MAX_RUNTIME_SECONDS, TimeUnit.SECONDS); // let running processes die.
    LOGGER.info("Shut down process execution.");
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
    RocksDB taskDb = RocksDB.open(dbOptions, "stores/task" + "/" + taskId);
    RocksDB replicator0Db = RocksDB.open(dbOptions, "stores/replicator" + "/" + taskId + "0");
    RocksDB replicator1Db = RocksDB.open(dbOptions, "stores/replicator" + "/" + taskId + "1");
    RocksIterator taskDbIterator = taskDb.newIterator();
    taskDbIterator.seekToFirst();

    byte[] lastCommittedMessageId = Util.readFile(Paths.get("stores/task" + "/" + taskId + "/MESSAGE_ID"));
    int messageId = Ints.fromByteArray(lastCommittedMessageId);
    int maxValidKey = messageId * taskId + messageId;

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
        LOGGER.error("Verification error for key: " + intKey, e);
      }
    }
    taskDbIterator.close();
    taskDb.close();
    replicator0Db.close();
    replicator1Db.close();
  }
}
