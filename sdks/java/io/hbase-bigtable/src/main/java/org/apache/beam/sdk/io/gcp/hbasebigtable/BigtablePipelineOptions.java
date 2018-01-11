package org.apache.beam.sdk.io.gcp.hbasebigtable;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface BigtablePipelineOptions extends PipelineOptions {
  @Description("Bigtable instanceId")
  @Default.String("beam-test")
  ValueProvider<String> getInstanceId();
  void setInstanceId(ValueProvider<String> value);
  
  @Description("Bigtable projectId")
  ValueProvider<String> getProjectId();
  void setProjectId(ValueProvider<String> value);

  @Description("Bigtable tableId")
  ValueProvider<String> getTableId();
  void setTableId(ValueProvider<String> value);
}

