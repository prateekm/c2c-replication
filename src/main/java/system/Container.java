package system;

import util.Constants;
import util.Pair;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Container {
  private static final Logger LOGGER = LoggerFactory.getLogger(Container.class);
  private final Task task;
  private final Producer producer;
  private final List<Replicator> replicators;


  public static void main(String[] args) throws Exception {
    Files.createDirectories(Paths.get(Constants.TASK_STORE_BASE_PATH));
    Files.createDirectories(Paths.get(Constants.PRODUCER_STORE_BASE_PATH));
    Files.createDirectories(Paths.get(Constants.REPLICATOR_STORE_BASE_PATH));

    int containerId = Integer.valueOf(args[0]);
    ContainerModel containerModel = Constants.JOB_MODEL.getContainerModel(containerId);
    LOGGER.info(containerModel.toString());
    Integer taskId = containerModel.getTaskId();

    try {
      RocksDB.loadLibrary();
      Options dbOptions = new Options().setCreateIfMissing(true);
      RocksDB taskDb = RocksDB.open(dbOptions, Constants.TASK_STORE_BASE_PATH + "/" + taskId);
      RocksDB producerDb = RocksDB.open(dbOptions, Constants.PRODUCER_STORE_BASE_PATH + "/" + taskId);

      Integer firstReplicaPort = Constants.JOB_MODEL.getReplicators().get(taskId + "0").right;
      Integer secondReplicaPort = Constants.JOB_MODEL.getReplicators().get(taskId + "1").right;
      Producer producer = new Producer(taskId, producerDb, firstReplicaPort, secondReplicaPort); // producer id == task id
      Task task = new Task(taskId, taskDb, producer);

      List<Replicator> replicators = new ArrayList<>();
      for (Map.Entry<String, Pair<Integer, Integer>> entry: Constants.JOB_MODEL.getReplicators().entrySet()) {
        if (entry.getValue().left.equals(taskId)) {
          String replicatorId = entry.getKey();
          Integer replicatorPort = entry.getValue().right;
          RocksDB replicatorDb = RocksDB.open(dbOptions, Constants.REPLICATOR_STORE_BASE_PATH + "/" + replicatorId);
          Replicator replicator = new Replicator(replicatorId, replicatorPort, replicatorDb);
          replicators.add(replicator);
        }
      }
      Container container = new Container(task, producer, replicators);
      container.start();
    } catch (Exception e) {
      LOGGER.error("Error starting container.", e);
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
