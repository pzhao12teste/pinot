package com.linkedin.pinot.controller.helix.core.rebalancer;

import com.linkedin.pinot.common.config.ReplicaGroupStrategyConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.controller.utils.ReplicaGroupSegmentAssignmentUtils;
import com.linkedin.pinot.controller.utils.SegmentMetadataMockUtils;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.model.IdealState;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class ReplicaGroupRebalanceStrategyTest extends ControllerTest {
  private static final int MIN_NUM_REPLICAS = 3;
  private static final int NUM_BROKER_INSTANCES = 2;
  private static final int NUM_SERVER_INSTANCES = 6;
  private static final String TABLE_NAME = "testReplicaRebalance";
  private final static String PARTITION_COLUMN = "memberId";

  private final TableConfig.Builder _offlineBuilder = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE);
  private String _createTableUrl;

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    ControllerConf config = getDefaultControllerConfiguration();
    config.setTableMinReplicas(MIN_NUM_REPLICAS);
    startController(config);
    _createTableUrl = _controllerRequestURLBuilder.forTableCreate();

    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, NUM_BROKER_INSTANCES, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, NUM_SERVER_INSTANCES, true);

    _offlineBuilder.setTableName("testOfflineTable")
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5");

    setUpReplicaGroupTable();
  }

  @Test
  public void testReplicaGroupRebalancer() throws Exception {
    // Check replacement case
    int newNumInstancePerPartition = 3;
    int newNumReplicaGroup = 2;

    _helixResourceManager.dropInstance("Server_localhost_0");
    ControllerRequestBuilderUtil.addFakeDataInstanceToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, "Server_localhost_a", true);

    _helixResourceManager.rebalanceReplicaGroupTable(TABLE_NAME,
        CommonConstants.Helix.TableType.OFFLINE, newNumInstancePerPartition, newNumReplicaGroup, false);

    // Check adding a server to each replica
    newNumInstancePerPartition = 4;
    newNumReplicaGroup = 2;

    // Add 2 more servers
    ControllerRequestBuilderUtil.addFakeDataInstanceToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, "Server_localhost_b", true);
    ControllerRequestBuilderUtil.addFakeDataInstanceToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, "Server_localhost_c", true);

//    _helixResourceManager.rebalanceReplicaGroupTable("federator2", CommonConstants.Helix.TableType.OFFLINE, newNumInstancePerPartition, newNumReplicaGroup, false);

    Thread.sleep(1000000);
  }



  private void setUpReplicaGroupTable() throws Exception {
    // Create the configuration for segment assignment strategy.
    int numInstancesPerPartition = 3;
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig();
    replicaGroupStrategyConfig.setNumInstancesPerPartition(numInstancesPerPartition);
    replicaGroupStrategyConfig.setMirrorAssignmentAcrossReplicaGroups(true);

    // Create table config
    TableConfig tableConfig = new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE).setTableName(
        TABLE_NAME)
        .setNumReplicas(2)
        .setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy")
        .build();
    tableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupStrategyConfig);

    // Create the table and upload segments
    _helixResourceManager.addTable(tableConfig);

    // Wait for table addition
    while (!_helixResourceManager.hasOfflineTable(TABLE_NAME)) {
      Thread.sleep(100);
    }

    // Upload 10 segments
    int numSegments = 10;
    Set<String> segments =
        ReplicaGroupSegmentAssignmentUtils.uploadMultipleSegmentsWithSinglePartitionNumber(TABLE_NAME, numSegments,
            PARTITION_COLUMN, _helixResourceManager);
    // Wait for all segments appear in the external view
    while (!allSegmentsPushedToIdealState(TABLE_NAME, numSegments)) {
      Thread.sleep(100);
    }
  }

  private boolean allSegmentsPushedToIdealState(String tableName, int segmentNum) {
    IdealState idealState =
        _helixAdmin.getResourceIdealState(getHelixClusterName(), TableNameBuilder.OFFLINE.tableNameWithType(tableName));
    if (idealState != null && idealState.getPartitionSet() != null
        && idealState.getPartitionSet().size() == segmentNum) {
      return true;
    }
    return false;
  }
}
