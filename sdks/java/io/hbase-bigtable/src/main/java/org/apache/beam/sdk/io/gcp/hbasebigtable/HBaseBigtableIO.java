package org.apache.beam.sdk.io.gcp.hbasebigtable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.hbase.AbstractHBaseIO;
import org.apache.beam.sdk.io.hbase.SerializableScan;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.bigtable.repackaged.com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.repackaged.com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.config.BigtableOptions;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;

/**
 * A bounded source and sink for Google Cloud Bigtable.
 *
 * <p>For more information about Cloud Bigtable, see the online documentation at
 * <a href="https://cloud.google.com/bigtable/">Google Cloud Bigtable</a>.
 *
 * <h3>Reading from Cloud Bigtable</h3>
 *
 * <p>The Bigtable source returns a set of rows from a single table, returning a {@code
 * PCollection<Result>}.
 *
 * <p>To configure a Cloud Bigtable source, you must supply a table id, a project id, an instance
 * id and optionally configuring {@link BigtableOptionsFactory} properties using 
 * {@link HBaseBigtableIO.Read#withConfiguration} to provide more specific connection configuration.
 * By default, {@link HBaseBigtableIO.Read} will read all rows in the table. The row range to be read
 * can optionally be restricted using {@link HBaseBigtableIO.Read#withKeyRange}, and a {@link RowFilter}
 * can be specified using {@link HBaseBigtableIO.Read#withRowFilter}. For example:
 *
 * <pre>{@code
 * // Scan the entire table.
 * p.apply("read",
 *     HBaseBigtableIO.read()
 *          .withProjectId(projectId)
 *          .withInstanceId(instanceId)
 *          .withTableId("table"));
 *
 * // Filter data using a HBaseIO Scan
 * Scan scan = ...
 * p.apply("read",
 *     HBaseBigtableIO.read()
 *          .withProjectId(projectId)
 *          .withInstanceId(instanceId)
 *          .withTableId("table"));
 *          .withScan(scan));
 *
 * // Scan a prefix of the table.
 * ByteKeyRange keyRange = ...;
 * p.apply("read",
 *     HBaseBigtableIO.read()
 *          .withProjectId(projectId)
 *          .withInstanceId(instanceId)
 *          .withTableId("table"));
 *          .withKeyRange(keyRange));
 *
 * // Scan a subset of rows that match the specified row filter.
 * p.apply("filtered read",
 *     HBaseBigtableIO.read()
 *          .withProjectId(projectId)
 *          .withInstanceId(instanceId)
 *          .withTableId("table"));
 *          .withFilter(filter));
 * }</pre>
 *
 * <h3>Writing to Cloud Bigtable</h3>
 *
 * <p>The Bigtable sink executes a set of row mutations on a single table. It takes as input a {@link
 * PCollection PCollection&lt;Mutation&gt;}, where each {@link Mutation} represents an idempotent
 * transformation on a row.
 *
 * <p>To configure a Bigtable sink, you must supply a a table id, a project id, and an instance id to identify
 * the Bigtable instance, for example:
 *
 * <pre>{@code
 * PCollection<Mutation> data = ...;
 *
 * data.apply("write",
 *     HBaseBigtableIO.write()
 *          .withProjectId(projectId)
 *          .withInstanceId(instanceId)
 *          .withTableId("table"));
 * }</pre>
 *
 * <h3>Experimental</h3>
 *
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class HBaseBigtableIO extends AbstractHBaseIO {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseBigtableIO.class);

  /** Disallow construction of utility class. */
  private HBaseBigtableIO() {
  }
  
  /**
   * Creates an uninitialized {@link HBaseBigtableIO.Read}. Before use, the {@code Read} must be initialized
   * with a a {@link HBaseBigtableIO.Write#withProjectId(String)}, {@link HBaseBigtableIO.Write#withInstanceId(String)}
   * which specifies the HBase instance, and a {@link HBaseBigtableIO.Read#withTableId tableId} which specifies 
   * which table to read. A {@link Filter} may also optionally be specified using {@link HBaseBigtableIO.Read#withFilter}.
   */
  @Experimental
  public static Read read() {
    Configuration conf = new Configuration(false);
    conf.set("hbase.client.connection.impl",
        BigtableConfiguration.getConnectionClass().getCanonicalName());
    
    return new Read(new SerializableConfiguration(conf),
        ValueProvider.StaticValueProvider.of(""), new SerializableScan(new Scan()));
  }

  /**
   * A {@link PTransform} that reads from Cloud Bigtable. See the class-level Javadoc on {@link HBaseBigtableIO} for
   * more information.
   *
   * @see HBaseBigtableIO
   */
  protected static class Read extends AbstractHBaseIO.AbstractRead {
    private static final long serialVersionUID = 0L;
    
    protected Read(SerializableConfiguration serializableConfiguration,
        ValueProvider<String> tableId,
        SerializableScan serializableScan) {
      super(serializableConfiguration, tableId, serializableScan);
    }

    public Read withTableId(String tableId) {
      checkArgument(tableId != null, "tableIdcan not be null");
      return new Read(serializableConfiguration, StaticValueProvider.of(tableId),
          serializableScan);
    }

    public Read withTableId(ValueProvider<String> tableId) {
      checkArgument(tableId != null, "tableIdcan not be null");
      return new Read(serializableConfiguration, tableId, serializableScan);
    }

    public Read withProjectId(String projectId) {
      checkArgument(projectId != null, "projectId can not be null");
      return withConfiguration(BigtableOptionsFactory.PROJECT_ID_KEY, projectId);
    }

    public Read withProjectId(ValueProvider<String> projectId) {
      checkArgument(projectId != null, "projectId can not be null");
      checkArgument(!projectId.isAccessible(), "Invlaid access to RuntimeValueProvider instanceId");
      return withConfiguration(BigtableOptionsFactory.PROJECT_ID_KEY, projectId.get());
    }

    public Read withInstanceId(String instanceId) {
      checkArgument(instanceId != null, "instanceId can not be null");
      return withConfiguration(BigtableOptionsFactory.INSTANCE_ID_KEY, instanceId);
    }

    public Read withInstanceId(ValueProvider<String> instanceId) {
      checkArgument(instanceId != null, "instanceId can not be null");
      checkArgument(!instanceId.isAccessible(), "Invlaid access to RuntimeValueProvider instanceId");
      return withConfiguration(BigtableOptionsFactory.INSTANCE_ID_KEY, instanceId.get());
    }

    public Read withScan(Scan scan) {
      checkArgument(scan != null, "scancan not be null");
      return new Read(serializableConfiguration, tableId, serializableScan);
    }

    public Read withFilter(Filter filter) {
      checkArgument(filter != null, "filtercan not be null");
      return withScan(this.serializableScan.get().setFilter(filter));
    }

    /** Reads only rows in the specified range. */
    public Read withKeyRange(ByteKeyRange keyRange) {
      checkArgument(keyRange != null, "keyRangecan not be null");
      byte[] startRow = keyRange.getStartKey().getBytes();
      byte[] stopRow = keyRange.getEndKey().getBytes();
      return withScan(serializableScan.get().setStartRow(startRow).setStopRow(stopRow));
    }

    /** Reads only rows in the specified range. */
    public Read withKeyRange(byte[] startRow, byte[] stopRow) {
      checkArgument(startRow != null, "startRowcan not be null");
      checkArgument(stopRow != null, "stopRowcan not be null");
      ByteKeyRange keyRange =
          ByteKeyRange.of(ByteKey.copyFrom(startRow), ByteKey.copyFrom(stopRow));
      return withKeyRange(keyRange);
    }

    public Read withConfiguration(String key, String value) {
      checkArgument(value != null, "value can not be null");
      checkArgument(serializableConfiguration != null, "serializableConfiguration can not be null");
      
      Configuration conf = serializableConfiguration.get();
      conf.set(key, value);
      return new Read(serializableConfiguration, tableId, serializableScan);
    }

    public String getProjectId() {
      return serializableConfiguration != null
          ? serializableConfiguration.get().get(BigtableOptionsFactory.PROJECT_ID_KEY) : null;
    }

    public String getInstanceId() {
      return serializableConfiguration != null
          ? serializableConfiguration.get().get(BigtableOptionsFactory.INSTANCE_ID_KEY) : null;
    }

    @Override
    public void validate() {
      super.validate();
      checkArgument(getProjectId() != null, "withProjectId() is required");
      checkArgument(getInstanceId() != null, "withInstanceId() is required");
    }
    
    @Override
    public AbstractSource createSource() {
      return new BigtableSource(this, null /* estimatedSizeBytes */);
    }
    
    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("projectId", getProjectId()));
      builder.add(DisplayData.item("instanceId", getInstanceId()));
    }
  }

  protected static class BigtableSource extends AbstractHBaseIO.AbstractSource {
    private static final long serialVersionUID = 1L;
    
    protected static final long SIZED_BASED_MAX_SPLIT_COUNT = 4_000;
    static final long COUNT_MAX_SPLIT_COUNT = 15_360;

    public BigtableSource(Read read, @Nullable Long estimatedSizeBytes) {
      super(read, estimatedSizeBytes);
    }

    @Override
    protected BigtableSource withStartKey(ByteKey startKey) throws IOException {
      checkNotNull(startKey, "startKey");
      Read newRead = new Read(read.getSerializableConfiguration(), read.getTableId(),
          new SerializableScan(
              new Scan(read.getSerializableScan().get()).setStartRow(startKey.getBytes())));
      return new BigtableSource(newRead, estimatedSizeBytes);
    }

    @Override
    protected BigtableSource withEndKey(ByteKey endKey) throws IOException {
      checkNotNull(endKey, "endKey");
      Read newRead = new Read(read.getSerializableConfiguration(), read.getTableId(),
          new SerializableScan(
              new Scan(read.getSerializableScan().get()).setStopRow(endKey.getBytes())));
      return new BigtableSource(newRead, estimatedSizeBytes);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      long totalEstimatedSizeBytes = 0;

      final Scan scan = read.getSerializableScan().get();
      byte[] scanStartKey = scan.getStartRow();
      byte[] scanStopKey = scan.getStopRow();

      byte[] startKey = HConstants.EMPTY_START_ROW;
      long lastOffset = 0;
      for (SampleRowKeysResponse response : getSampleRowKeys()) {
        byte[] currentEndKey = response.getRowKey().toByteArray();
        // Avoid empty regions.
        if (Bytes.equals(startKey, currentEndKey) && startKey.length != 0) {
          continue;
        }
        long offset = response.getOffsetBytes();
        if (isWithinRange(scanStartKey, scanStopKey, startKey, currentEndKey)) {
          totalEstimatedSizeBytes += (offset - lastOffset);
        }
        lastOffset = offset;
        startKey = currentEndKey;
      }
      LOG.info("Estimated size in bytes: " + totalEstimatedSizeBytes);

      return totalEstimatedSizeBytes;
    }

    private List<SampleRowKeysResponse> getSampleRowKeys() throws IOException {
      BigtableOptions bigtableOptions = BigtableOptionsFactory
          .fromConfiguration(read.getSerializableConfiguration().get());
      try (BigtableSession session = new BigtableSession(bigtableOptions)) {
        BigtableTableName tableName = bigtableOptions.getInstanceName().toTableName(read.getTableId().get());
        SampleRowKeysRequest request = SampleRowKeysRequest.newBuilder()
            .setTableName(tableName.toString()).build();
        return session.getDataClient().sampleRowKeys(request);
      }
    }

    private static boolean isWithinRange(byte[] scanStartKey, byte[] scanEndKey, byte[] startKey,
        byte[] endKey) {
      return (scanStartKey.length == 0 || endKey.length == 0
          || Bytes.compareTo(scanStartKey, endKey) < 0)
          && (scanEndKey.length == 0 || Bytes.compareTo(scanEndKey, startKey) > 0);
    }

    @Override
    public BoundedReader<Result> createReader(PipelineOptions pipelineOptions) throws IOException {
      return new BigtableReader(this);
    }

    @Override
    public AbstractSource createSource(SerializableScan serializableScan, Long estimatedSizeBytes) {
      return new BigtableSource(new Read(this.read.getSerializableConfiguration(),
          this.read.getTableId(), serializableScan), estimatedSizeBytes);
    }
  }

  protected static class BigtableReader extends AbstractHBaseIO.AbstractReader {
    BigtableReader(BigtableSource source) {
      super(source);
    }
  }
  
  /**
   * Creates an uninitialized {@link HBaseBigtableIO.Write}. Before use, the {@code Write} must be
   * initialized with a {@link HBaseBigtableIO.Write#withProjectId(String)}, 
   * {@link HBaseBigtableIO.Write#withInstanceId(String)} that specifies the destination Bigtable
   * instance, and a {@link HBaseBigtableIO.Write#withTableId tableId} that specifies
   * which table to write.
   */
  public static Write write() {
    Configuration conf = new Configuration(false);
    conf.set("hbase.client.connection.impl",
        BigtableConfiguration.getConnectionClass().getCanonicalName());

    return new Write(new SerializableConfiguration(conf),
        ValueProvider.StaticValueProvider.of(""));
  }

  /**
   * A {@link PTransform} that writes to Cloud Bigtable. See the class-level Javadoc on {@link HBaseBigtableIO}
   * more information.
   *
   * @see HBaseBigtableIO
   */
  public static class Write extends AbstractHBaseIO.AbstractWrite {
    private static final long serialVersionUID = 0L;
    
    public Write(SerializableConfiguration serializableConfiguration,
        ValueProvider<String> tableId) {
      super(serializableConfiguration, tableId);
    }

    public Write withTableId(String tableId) {
      checkArgument(tableId != null, "tableIdcan not be null");
      return new Write(serializableConfiguration, StaticValueProvider.of(tableId));
    }

    public Write withTableId(ValueProvider<String> tableId) {
      checkArgument(tableId != null, "tableIdcan not be null");
      return new Write(serializableConfiguration, tableId);
    }

    public Write withProjectId(String projectId) {
      checkArgument(projectId != null, "projectId can not be null");
      return withConfiguration(BigtableOptionsFactory.PROJECT_ID_KEY, projectId);
    }

    public Write withProjectId(ValueProvider<String> projectId) {
      checkArgument(projectId != null, "projectId can not be null");
      checkArgument(!projectId.isAccessible(), "Invlaid access to RuntimeValueProvider instanceId");
      return withConfiguration(BigtableOptionsFactory.PROJECT_ID_KEY, projectId.get());
    }

    public Write withInstanceId(String instanceId) {
      checkArgument(instanceId != null, "instanceId can not be null");
      return withConfiguration(BigtableOptionsFactory.INSTANCE_ID_KEY, instanceId);
    }

    public Write withInstanceId(ValueProvider<String> instanceId) {
      checkArgument(instanceId != null, "instanceId can not be null");
      checkArgument(!instanceId.isAccessible(), "Invlaid access to RuntimeValueProvider instanceId");
      return withConfiguration(BigtableOptionsFactory.INSTANCE_ID_KEY, instanceId.get());
    }

    public Write withConfiguration(String key, String value) {
      checkArgument(value != null, "value can not be null");
      checkArgument(serializableConfiguration != null, "serializableConfiguration can not be null");

      Configuration conf = serializableConfiguration.get();
      conf.set(key, value);
      return new Write(serializableConfiguration, tableId);
    }

    public String getProjectId() {
      return serializableConfiguration != null
          ? serializableConfiguration.get().get(BigtableOptionsFactory.PROJECT_ID_KEY) : null;
    }

    public String getInstanceId() {
      return serializableConfiguration != null
          ? serializableConfiguration.get().get(BigtableOptionsFactory.INSTANCE_ID_KEY) : null;
    }

    @Override
    public void validate() {
      super.validate();
      checkArgument(getProjectId() != null, "withProjectId() is required");
      checkArgument(getInstanceId() != null, "withInstanceId() is required");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("projectId", getProjectId()));
      builder.add(DisplayData.item("instanceId", getInstanceId()));
    }
  }
}
