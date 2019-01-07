package system;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import util.Constants;
import util.Pair;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;

public class Container {
  static {
    System.setProperty(SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
    System.setProperty(SimpleLogger.SHOW_DATE_TIME_KEY, "true");
    System.setProperty(SimpleLogger.DATE_TIME_FORMAT_KEY, "hh:mm:ss:SSS");
    System.setProperty(SimpleLogger.SHOW_THREAD_NAME_KEY, "false");
    System.setProperty(SimpleLogger.SHOW_SHORT_LOG_NAME_KEY, "true");
    System.setProperty(SimpleLogger.LEVEL_IN_BRACKETS_KEY, "true");
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Container.class);
  private final Task task;
  private final Producer producer;
  private final List<Replicator> replicators;

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
      LOGGER.error("Uncaught exception in Thread: {}. Prematurely exiting process.", t.getName(), e);
      System.exit(1);
    });

    setConfiguration(args);

    Files.createDirectories(Paths.get(Constants.Common.getTaskStoreBasePath()));
    Files.createDirectories(Paths.get(Constants.Common.getProducerStoreBasePath()));
    Files.createDirectories(Paths.get(Constants.Common.getReplicatorStoreBasePath()));

    int containerId = Integer.valueOf(args[0]);
    ContainerModel containerModel = Constants.Common.JOB_MODEL.getContainerModel(containerId);
    LOGGER.info("Container: {} is starting with {}",  containerId, containerModel);
    Integer taskId = containerModel.getTaskId();

    try {
      RocksDB.loadLibrary();
      RocksDB taskDb = RocksDB.open(Constants.Common.DB_OPTIONS, Constants.Common.getTaskStoreBasePath() + "/" + taskId);
      RocksDB producerDb = RocksDB.open(Constants.Common.DB_OPTIONS, Constants.Common.getProducerStoreBasePath() + "/" + taskId);

      Producer producer = new Producer(taskId, producerDb, taskDb); // producer id == task id
      Task task = new Task(taskId, taskDb, producer);

      List<Replicator> replicators = new ArrayList<>();
      for (Map.Entry<String, Pair<Integer, Integer>> entry: Constants.Common.JOB_MODEL.getReplicators().entrySet()) {
        if (entry.getValue().left.equals(taskId)) {
          String replicatorId = entry.getKey();
          Replicator replicator = new Replicator(replicatorId);
          replicators.add(replicator);
        }
      }
      Container container = new Container(task, producer, replicators);
      container.start();
    } catch (Exception e) {
      LOGGER.error("Error starting container. Prematurely exiting process.", e);
    }
  }

  public static void setConfiguration(String[] args) {
    OptionParser parser = new OptionParser();
    parser.accepts("execution-id").withOptionalArg().ofType(Integer.class);

    OptionSet options = parser.parse(args);
    if (options.hasArgument("execution-id")) {
      Constants.Common.EXECUTION_ID = (int) options.valueOf("execution-id");
    }
  }

  public Container(Task task, Producer producer, List<Replicator> replicators) {
    this.task = task;
    this.producer = producer;
    this.replicators = replicators;
  }

  public void start() {
    producer.start();
    replicators.forEach(Replicator::start);
    task.start();
  }
}
