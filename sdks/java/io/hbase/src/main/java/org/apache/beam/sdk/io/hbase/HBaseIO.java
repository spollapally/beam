/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.hbase;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bounded source and sink for HBase.
 *
 * <p>For more information, see the online documentation at <a
 * href="https://hbase.apache.org/">HBase</a>.
 *
 * <h3>Reading from HBase</h3>
 *
 * <p>The HBase source returns a set of rows from a single table, returning a {@code
 * PCollection<Result>}.
 *
 * <p>To configure a HBase source, you must supply a table id and a {@link Configuration} to
 * identify the HBase instance. By default, {@link HBaseIO.Read} will read all rows in the table.
 * The row range to be read can optionally be restricted using with a {@link Scan} object or using
 * the {@link HBaseIO.Read#withKeyRange}, and a {@link Filter} using {@link
 * HBaseIO.Read#withFilter}, for example:
 *
 * <pre>{@code
 * // Scan the entire table.
 * p.apply("read",
 *     HBaseIO.read()
 *         .withConfiguration(configuration)
 *         .withTableId("table"));
 *
 * // Filter data using a HBaseIO Scan
 * Scan scan = ...
 * p.apply("read",
 *     HBaseIO.read()
 *         .withConfiguration(configuration)
 *         .withTableId("table"))
 *         .withScan(scan));
 *
 * // Scan a prefix of the table.
 * ByteKeyRange keyRange = ...;
 * p.apply("read",
 *     HBaseIO.read()
 *         .withConfiguration(configuration)
 *         .withTableId("table")
 *         .withKeyRange(keyRange));
 *
 * // Scan a subset of rows that match the specified row filter.
 * p.apply("filtered read",
 *     HBaseIO.read()
 *         .withConfiguration(configuration)
 *         .withTableId("table")
 *         .withFilter(filter));
 * }</pre>
 *
 * <h3>Writing to HBase</h3>
 *
 * <p>The HBase sink executes a set of row mutations on a single table. It takes as input a {@link
 * PCollection PCollection&lt;Mutation&gt;}, where each {@link Mutation} represents an idempotent
 * transformation on a row.
 *
 * <p>To configure a HBase sink, you must supply a table id and a {@link Configuration} to identify
 * the HBase instance, for example:
 *
 * <pre>{@code
 * Configuration configuration = ...;
 * PCollection<Mutation> data = ...;
 *
 * data.apply("write",
 *     HBaseIO.write()
 *         .withConfiguration(configuration)
 *         .withTableId("table"));
 * }</pre>
 *
 * <h3>Experimental</h3>
 *
 * <p>The design of the API for HBaseIO is currently related to the BigtableIO one, it can evolve or
 * be different in some aspects, but the idea is that users can easily migrate from one to the other
 * .
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class HBaseIO extends AbstractHBaseIO {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseIO.class);

  /** Disallow construction of utility class. */
  private HBaseIO() {}

  /**
   * Creates an uninitialized {@link HBaseIO.Read}. Before use, the {@code Read} must be initialized
   * with a {@link HBaseIO.Read#withConfiguration(Configuration)} that specifies the HBase instance,
   * and a {@link HBaseIO.Read#withTableId tableId} that specifies which table to read. A {@link
   * Filter} may also optionally be specified using {@link HBaseIO.Read#withFilter}.
   */
  @Experimental
  public static Read read() {
    return new Read(null, ValueProvider.StaticValueProvider.of(""), new SerializableScan(new Scan()));
  }

  /**
   * A {@link PTransform} that reads from HBase. See the class-level Javadoc on {@link HBaseIO} for*
   * more information.
   *
   * @see HBaseIO
   */
  public static class Read extends AbstractHBaseIO.AbstractRead {
   private static final long serialVersionUID = 0L;
 
    private Read(SerializableConfiguration serializableConfiguration, ValueProvider<String> tableId,
        SerializableScan serializableScan) {
      super(serializableConfiguration, tableId, serializableScan);
    }
    
    /** Reads from the HBase instance indicated by the* given configuration. */
    public Read withConfiguration(Configuration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return new Read(new SerializableConfiguration(configuration), tableId, serializableScan);
    }

    /** Reads from the specified table. */
    public Read withTableId(String tableId) {
      checkArgument(tableId != null, "tableId can not be null");
      return new Read(serializableConfiguration, StaticValueProvider.of(tableId), serializableScan);
    }

    public Read withTableId(ValueProvider<String> tableId) {
      checkArgument(tableId != null, "tableId can not be null");
      return new Read(serializableConfiguration, tableId, serializableScan);
    }

    /** Filters the rows read from HBase using the given* scan. */
    public Read withScan(Scan scan) {
      checkArgument(scan != null, "scancan not be null");
      return new Read(serializableConfiguration, tableId, new SerializableScan(scan));
    }

    /** Filters the rows read from HBase using the given* row filter. */
    public Read withFilter(Filter filter) {
      checkArgument(filter != null, "filtercan not be null");
      return withScan(serializableScan.get().setFilter(filter));
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
 
    @Override
    public BoundedSource<Result> createSource() {
      return new HBaseSource(this, null /* estimatedSizeBytes */);
    }
  }

  public static class HBaseSource extends AbstractHBaseIO.AbstractSource {
    private static final long serialVersionUID = 1L;

    public HBaseSource(Read read, @Nullable Long estimatedSizeBytes) {
      super(read, estimatedSizeBytes);
    }

    @Override
    protected HBaseSource withStartKey(ByteKey startKey) throws IOException {
      checkNotNull(startKey, "startKey");
      Read newRead =
          new Read(
              read.serializableConfiguration,
              read.tableId,
              new SerializableScan(
                  new Scan(read.serializableScan.get()).setStartRow(startKey.getBytes())));
      return new HBaseSource(newRead, estimatedSizeBytes);
    }

    @Override
    protected HBaseSource withEndKey(ByteKey endKey) throws IOException {
      checkNotNull(endKey, "endKey");
      Read newRead =
          new Read(
              read.serializableConfiguration,
              read.tableId,
              new SerializableScan(
                  new Scan(read.serializableScan.get()).setStopRow(endKey.getBytes())));
      return new HBaseSource(newRead, estimatedSizeBytes);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
      if (estimatedSizeBytes == null) {
        estimatedSizeBytes = estimateSizeBytes();
        LOG.debug(
            "Estimated size {} bytes for table {} and scan {}",
            estimatedSizeBytes,
            read.tableId,
            read.serializableScan.get());
      }
      return estimatedSizeBytes;
    }

    /**
     * This estimates the real size, it can be the compressed size depending on the HBase
     * configuration.
     */
    private long estimateSizeBytes() throws Exception {
      // This code is based on RegionSizeCalculator in hbase-server
      long estimatedSizeBytes = 0L;
      Configuration configuration = this.read.serializableConfiguration.get();
      try (Connection connection = ConnectionFactory.createConnection(configuration)) {
        // filter regions for the given table/scan
        List<HRegionLocation> regionLocations = getRegionLocations(connection);

        // builds set of regions who are part of the table scan
        Set<byte[]> tableRegions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (HRegionLocation regionLocation : regionLocations) {
          tableRegions.add(regionLocation.getRegionInfo().getRegionName());
        }

        // calculate estimated size for the regions
        Admin admin = connection.getAdmin();
        ClusterStatus clusterStatus = admin.getClusterStatus();
        Collection<ServerName> servers = clusterStatus.getServers();
        for (ServerName serverName : servers) {
          ServerLoad serverLoad = clusterStatus.getLoad(serverName);
          for (RegionLoad regionLoad : serverLoad.getRegionsLoad().values()) {
            byte[] regionId = regionLoad.getName();
            if (tableRegions.contains(regionId)) {
              long regionSizeBytes = regionLoad.getStorefileSizeMB() * 1_048_576L;
              estimatedSizeBytes += regionSizeBytes;
            }
          }
        }
      }
      return estimatedSizeBytes;
    }

    @Override
    public BoundedReader<Result> createReader(PipelineOptions pipelineOptions) throws IOException {
      return new HBaseReader(this);
    }

    @Override
    protected AbstractSource createSource(SerializableScan serializableScan,
        Long estimatedSizeBytes) {
      return new HBaseSource(new Read(this.read.getSerializableConfiguration(),
          this.read.getTableId(), serializableScan), estimatedSizeBytes);
    }
  }

  private static class HBaseReader extends AbstractHBaseIO.AbstractReader {

    HBaseReader(HBaseSource source) {
      super(source);
    }
  }

  /**
   * Creates an uninitialized {@link HBaseIO.Write}. Before use, the {@code Write} must be
   * initialized with a {@link HBaseIO.Write#withConfiguration(Configuration)} that specifies the
   * destination HBase instance, and a {@link HBaseIO.Write#withTableId tableId} that specifies
   * which table to write.
   */
  public static Write write() {
    return new Write(null /* SerializableConfiguration */,
        ValueProvider.StaticValueProvider.of(""));
  }

  /**
   * A {@link PTransform} that writes to HBase. See the class-level Javadoc on {@link HBaseIO} for*
   * more information.
   *
   * @see HBaseIO
   */
  public static class Write extends AbstractHBaseIO.AbstractWrite {
    private static final long serialVersionUID = 0L;
    
    public Write(SerializableConfiguration serializableConfiguration,
        ValueProvider<String> tableId) {
      super(serializableConfiguration, tableId);
    }

    /** Writes to the HBase instance indicated by the* given Configuration. */
    public Write withConfiguration(Configuration configuration) {
      checkArgument(configuration != null, "configuration can not be null");
      return new Write(new SerializableConfiguration(configuration), tableId);
    }

    /** Writes to the specified table. */
    public Write withTableId(String tableId) {
      checkArgument(tableId != null, "tableIdcan not be null");
      return new Write(serializableConfiguration, StaticValueProvider.of(tableId));
    }

    public Write withTableId(ValueProvider<String> tableId) {
      checkArgument(tableId != null, "tableIdcan not be null");
      return new Write(serializableConfiguration, tableId);
    }
  }
}
