package com.linkedin.pinot.controller.utils;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class ReplicaGroupSegmentAssignmentUtils {
  private ReplicaGroupSegmentAssignmentUtils(){
  }

  public static Set<String> uploadMultipleSegmentsWithSinglePartitionNumber(String tableName, int numSegments,
      String partitionColumn, PinotHelixResourceManager resourceManager) {
    Set<String> segments = new HashSet<>();
    for (int i = 0; i < numSegments; ++i) {
      String segmentName = "segment" + i;
      SegmentMetadata segmentMetadata =
          SegmentMetadataMockUtils.mockSegmentMetadataWithPartitionInfo(tableName, segmentName, partitionColumn,
              0);
      resourceManager.addNewSegment(segmentMetadata, "downloadUrl");
      segments.add(segmentName);
    }
    return segments;
  }
}
