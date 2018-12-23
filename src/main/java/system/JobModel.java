package system;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import util.Pair;

import java.util.Map;

public class JobModel {
  Map<Integer, ContainerModel> containerModels; // containerId => containerModel
  Map<String, Pair<Integer, Integer>> replicators;

  public JobModel() {
    this.containerModels =
        ImmutableMap.of(
            0, new ContainerModel(0),
            1, new ContainerModel(1),
            2, new ContainerModel(2)
        );

    // replicator id = (source producer, num replica) (e.g 00 == first replica for producer 0)
    // pair = (on container, with port)
    this.replicators = ImmutableMap.<String, Pair<Integer, Integer>>builder()
        .put("00", new Pair<>(1, 10000))
        .put("01", new Pair<>(2, 10001))
        .put("10", new Pair<>(0, 10002))
        .put("11", new Pair<>(2, 10003))
        .put("20", new Pair<>(0, 10004))
        .put("21", new Pair<>(1, 10005))
        .build();
  }

  public ContainerModel getContainerModel(Integer containerId) {
    return containerModels.get(containerId);
  }

  public Map<String, Pair<Integer, Integer>> getReplicators() {
    return replicators;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("containerModels", containerModels)
        .add("replicators", replicators)
        .toString();
  }
}

class ContainerModel {
  Integer containerId;
  Integer taskId;

  public ContainerModel(Integer containerId) {
    this.containerId = containerId;
    this.taskId = containerId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("containerId", containerId)
        .add("taskId", taskId)
        .toString();
  }

  public Integer getContainerId() {
    return containerId;
  }

  public Integer getTaskId() {
    return taskId;
  }
}
