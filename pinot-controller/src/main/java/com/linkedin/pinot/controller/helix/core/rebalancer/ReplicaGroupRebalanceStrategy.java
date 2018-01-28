package com.linkedin.pinot.controller.helix.core.rebalancer;

import com.linkedin.pinot.common.metadata.segment.PartitionToReplicaGroupMappingZKMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy;
import org.apache.helix.controller.stages.ClusterDataCache;

public class ReplicaGroupRebalanceStrategy implements RebalanceStrategy {
  private int _targetNumReplicaGroup;
  private int _targetNumInstancesPerPartition;
  private boolean _dryrun;

  private String _resourceName;
  private List<String> _partitions;
  private LinkedHashMap<String, Integer> _states;
  private int _maximumPerNode;

  public ReplicaGroupRebalanceStrategy(String resourceName, List<String> partitions,
      final LinkedHashMap<String, Integer> states, int maximumPerNode) {
    init(resourceName, partitions, states, maximumPerNode);
  }

  public ReplicaGroupRebalanceStrategy(String resourceName, final List<String> partitions,
      final LinkedHashMap<String, Integer> states) {
    this(resourceName, partitions, states, Integer.MAX_VALUE);
  }

  public ReplicaGroupRebalanceStrategy(String resourceName, final List<String> partitions,
      final LinkedHashMap<String, Integer> states, int targetNumInstancesPerPartition, int targetNumReplicaGroup,
      boolean dryrun) {
    this(resourceName, partitions, states);
    _targetNumInstancesPerPartition = targetNumInstancesPerPartition;
    _targetNumReplicaGroup = targetNumReplicaGroup;
    _dryrun = dryrun;
  }

  public ReplicaGroupRebalanceStrategy() {
  }

  @Override
  public void init(String resourceName, List<String> partitions, LinkedHashMap<String, Integer> states,
      int maximumPerNode) {
    _resourceName = resourceName;
    _partitions = partitions;
    _states = states;
    _maximumPerNode = maximumPerNode;
  }

  /**
   * Compute the new segment assignment
   * @param allNodes
   * @param liveNodes
   * @param currentMapping
   * @param clusterData
   * @return
   */
  @Override
  public ZNRecord computePartitionAssignment(List<String> allNodes, List<String> liveNodes,
      Map<String, Map<String, String>> currentMapping, ClusterDataCache clusterData) {

    return null;
  }

  /**
   * This will
   * @param oldReplicaGroupMapping
   * @param addedServers
   * @param removedServers
   * @param partitionNumber
   * @param numReplicaGroup
   * @param numInstancesPerPartition
   * @return
   */
  public PartitionToReplicaGroupMappingZKMetadata computeNewReplicaGroupMapping(
      PartitionToReplicaGroupMappingZKMetadata oldReplicaGroupMapping, List<String> addedServers,
      List<String> removedServers, int partitionNumber, int numReplicaGroup, int numInstancesPerPartition) {
    Map<String, String> oldToNewServerMapping = buildOldToNewServerMapping(removedServers, addedServers);

    PartitionToReplicaGroupMappingZKMetadata newReplicaGroupMapping = new PartitionToReplicaGroupMappingZKMetadata();
    newReplicaGroupMapping.setTableName(oldReplicaGroupMapping.getTableName());

    // Replace old to new instance from the replica group mapping
    for (int i = 0; i < partitionNumber ; i++) {
      for (int g = 0; g < numReplicaGroup; g++) {
        System.out.println("partition: " + i + " replica group: " + g);
        List<String> oldReplicaGroupServers = oldReplicaGroupMapping.getInstancesfromReplicaGroup(i, g);
        List<String> instances = new ArrayList<>();
        for (String oldServer: oldReplicaGroupServers) {
          String instance = oldToNewServerMapping.get(oldServer);
          instance = (instance != null) ? instance : oldServer;
          instances.add(instance);
        }
        System.out.println(i + "_" + g + ": " + Arrays.toString(instances.toArray()));
        newReplicaGroupMapping.setInstancesToReplicaGroup(i, g, instances);
      }
    }
    return newReplicaGroupMapping;
  }



  public Map<String, Map<String, String>> computeReplaceInstanceNewMapping(Map<String, Map<String, String>> oldMapping, List<String> addedServers, List<String> removedServers) {
    Map<String, Map<String, String>> newMapping = new HashMap<>();
    Map<String, String> oldToNewServerMapping = buildOldToNewServerMapping(removedServers, addedServers);

    // Use one to one mapping from removed server to new instance
    for(String segment: oldMapping.keySet()) {
      Map<String, String> newEntry = new HashMap<>();
      for(Map.Entry<String, String> entry : oldMapping.get(segment).entrySet()) {
        String oldServer = entry.getKey();
        String instance = oldToNewServerMapping.get(oldServer);
        instance = (instance != null) ? instance : oldServer;
        newEntry.put(instance, entry.getValue());
      }
      newMapping.put(segment, newEntry);
    }
    return newMapping;
  }

  public ZNRecord computeAddServersToReplicaGroup() {
    // Get the old
    return null;

  }

  public ZNRecord computeRemoveServersFromReplicaGroup() {
    return null;

  }

  public ZNRecord computeAddReplicaGroups() {
    return null;

  }

  public ZNRecord computeRemoveReplicaGroups() {
    return null;

  }

  private Map<String, String> buildOldToNewServerMapping(List<String> oldServers, List<String> newServers) {
    if (oldServers.size() != newServers.size()) {
      throw new IllegalArgumentException("Size of old server and new server has to be the same");
    }
    Collections.sort(oldServers);
    Collections.sort(newServers);
    Map<String, String> oldToNewServerMapping = new HashMap<>();
    for (int i = 0; i < newServers.size(); i++) {
      oldToNewServerMapping.put(oldServers.get(i), newServers.get(i));
    }
    return oldToNewServerMapping;
  }

}
