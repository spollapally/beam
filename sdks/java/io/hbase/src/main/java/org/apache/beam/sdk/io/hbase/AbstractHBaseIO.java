package org.apache.beam.sdk.io.hbase;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.io.range.ByteKeyRangeTracker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract bounded source and sink for HBase and Bigtable
 *
 */
public abstract class AbstractHBaseIO {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseIO.class);

  /**
   * See the class-level Javadoc on implementations {@link HBaseIO} and {@link HBaseBigtableIO}
   * for more information.
   */
  protected abstract static class AbstractRead extends PTransform<PBegin, PCollection<Result>> {
    private static final long serialVersionUID = 0L;

    protected final SerializableConfiguration serializableConfiguration;
    protected final ValueProvider<String> tableId;
    protected final SerializableScan serializableScan;

    protected AbstractRead(SerializableConfiguration serializableConfiguration,
        ValueProvider<String> tableId, SerializableScan serializableScan) {
      this.serializableConfiguration = serializableConfiguration;
      this.tableId = tableId;
      this.serializableScan = serializableScan;
    }

    public ValueProvider<String> getTableId() {
      return tableId;
    }

    public SerializableConfiguration getSerializableConfiguration() {
      return serializableConfiguration;
    }

    public Configuration getConfiguration() {
      return serializableConfiguration.get();
    }

    public SerializableScan getSerializableScan() {
      return serializableScan;
    }

    /** Returns the range of keys that will be read from the table. */
    public ByteKeyRange getKeyRange() {
      byte[] startRow = serializableScan.get().getStartRow();
      byte[] stopRow = serializableScan.get().getStopRow();
      return ByteKeyRange.of(ByteKey.copyFrom(startRow), ByteKey.copyFrom(stopRow));
    }

    public void validate() {
      checkArgument(serializableConfiguration != null, "withConfiguration() is required");
      checkArgument(!tableId.isAccessible() || tableId.get() != null, "withTableId() is required");
    }

    abstract public BoundedSource<Result> createSource();

    @Override
    public PCollection<Result> expand(PBegin input) {
      validate();
      try (Connection connection = ConnectionFactory
          .createConnection(serializableConfiguration.get())) {
        Admin admin = connection.getAdmin();
        checkArgument(admin.tableExists(TableName.valueOf(tableId.get())),
            "Table %s does not exist", tableId);
      } catch (IOException e) {
        LOG.warn("Error checking whether table {} exists; proceeding.", tableId, e);
      }

      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(createSource()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("configuration", serializableConfiguration.get().toString()));
      builder.add(DisplayData.item("tableId", tableId.get()));
      builder.addIfNotNull(DisplayData.item("scan", serializableScan.get().toString()));
    }
  }

  protected static abstract class AbstractSource extends BoundedSource<Result> {
    private static final long serialVersionUID = 0L;

    protected final AbstractRead read;
    @Nullable
    protected Long estimatedSizeBytes;

    protected AbstractSource(AbstractRead read, @Nullable Long estimatedSizeBytes) {
      this.read = read;
      this.estimatedSizeBytes = estimatedSizeBytes;
    }

    protected abstract AbstractSource withStartKey(ByteKey startKey) throws IOException;

    protected abstract AbstractSource withEndKey(ByteKey endKey) throws IOException;

    protected abstract AbstractSource createSource(SerializableScan serializableScan,
        Long estimatedSizeBytes);

    private List<HRegionLocation> getRegionLocations(Connection connection) throws Exception {
      final Scan scan = read.serializableScan.get();
      byte[] startRow = scan.getStartRow();
      byte[] stopRow = scan.getStopRow();

      final List<HRegionLocation> regionLocations = new ArrayList<>();

      final boolean scanWithNoLowerBound = startRow.length == 0;
      final boolean scanWithNoUpperBound = stopRow.length == 0;

      TableName tableName = TableName.valueOf(read.tableId.get());
      RegionLocator regionLocator = connection.getRegionLocator(tableName);
      List<HRegionLocation> tableRegionInfos = regionLocator.getAllRegionLocations();
      for (HRegionLocation regionLocation : tableRegionInfos) {
        final byte[] startKey = regionLocation.getRegionInfo().getStartKey();
        final byte[] endKey = regionLocation.getRegionInfo().getEndKey();
        boolean isLastRegion = endKey.length == 0;
        // filters regions who are part of the scan
        if ((scanWithNoLowerBound || isLastRegion || Bytes.compareTo(startRow, endKey) < 0)
            && (scanWithNoUpperBound || Bytes.compareTo(stopRow, startKey) > 0)) {
          regionLocations.add(regionLocation);
        }
      }

      return regionLocations;
    }

    private List<BoundedSource<Result>> splitBasedOnRegions(List<HRegionLocation> regionLocations,
        int numSplits) throws Exception {
      final Scan scan = read.serializableScan.get();
      byte[] startRow = scan.getStartRow();
      byte[] stopRow = scan.getStopRow();

      final List<BoundedSource<Result>> sources = new ArrayList<>(numSplits);
      final boolean scanWithNoLowerBound = startRow.length == 0;
      final boolean scanWithNoUpperBound = stopRow.length == 0;

      for (HRegionLocation regionLocation : regionLocations) {
        final byte[] startKey = regionLocation.getRegionInfo().getStartKey();
        final byte[] endKey = regionLocation.getRegionInfo().getEndKey();
        boolean isLastRegion = endKey.length == 0;
        String host = regionLocation.getHostnamePort();

        final byte[] splitStart = (scanWithNoLowerBound || Bytes.compareTo(startKey, startRow) >= 0)
            ? startKey : startRow;
        final byte[] splitStop = (scanWithNoUpperBound || Bytes.compareTo(endKey, stopRow) <= 0)
            && !isLastRegion ? endKey : stopRow;

        LOG.debug("{} {} {} {} {}", sources.size(), host, read.tableId, Bytes.toString(splitStart),
            Bytes.toString(splitStop));

        // We need to create a new copy of the scan and read to add the new ranges
        Scan newScan = new Scan(scan).setStartRow(splitStart).setStopRow(splitStop);
        sources.add(createSource(new SerializableScan(newScan), estimatedSizeBytes));
      }
      return sources;
    }

    @Override
    public List<? extends BoundedSource<Result>> split(long desiredBundleSizeBytes,
        PipelineOptions options) throws Exception {
      LOG.debug("desiredBundleSize {} bytes", desiredBundleSizeBytes);
      long estimatedSizeBytes = getEstimatedSizeBytes(options);
      int numSplits = 1;
      if (estimatedSizeBytes > 0 && desiredBundleSizeBytes > 0) {
        numSplits = (int) Math.ceil((double) estimatedSizeBytes / desiredBundleSizeBytes);
      }

      try (Connection connection = ConnectionFactory.createConnection(read.getConfiguration())) {
        List<HRegionLocation> regionLocations = getRegionLocations(connection);
        int realNumSplits = numSplits < regionLocations.size() ? regionLocations.size() : numSplits;
        LOG.debug("Suggested {} bundle(s) based on size", numSplits);
        LOG.debug("Suggested {} bundle(s) based on number of regions", regionLocations.size());
        final List<BoundedSource<Result>> sources = splitBasedOnRegions(regionLocations,
            realNumSplits);
        LOG.debug("Split into {} bundle(s)", sources.size());
        if (numSplits >= 1) {
          return sources;
        }
        return Collections.singletonList(this);
      }
    }

    @Override
    public void validate() {
      read.validate(null /* input */);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      read.populateDisplayData(builder);
    }

    @Override
    public Coder<Result> getOutputCoder() {
      return HBaseResultCoder.of();
    }
  }

  protected abstract static class AbstractReader extends BoundedSource.BoundedReader<Result> {
    private AbstractSource source;
    private Connection connection;
    private ResultScanner scanner;
    private Iterator<Result> iter;
    private Result current;
    private final ByteKeyRangeTracker rangeTracker;
    private long recordsReturned;

    protected AbstractReader(AbstractSource source) {
      this.source = source;
      Scan scan = source.read.serializableScan.get();
      ByteKeyRange range = ByteKeyRange.of(ByteKey.copyFrom(scan.getStartRow()),
          ByteKey.copyFrom(scan.getStopRow()));
      rangeTracker = ByteKeyRangeTracker.of(range);
    }

    @Override
    public boolean start() throws IOException {
      AbstractSource source = getCurrentSource();
      String tableId = source.read.getTableId().get();
      connection = ConnectionFactory.createConnection(source.read.getConfiguration());
      TableName tableName = TableName.valueOf(tableId);
      Table table = connection.getTable(tableName);
      // [BEAM-2319] We have to clone the Scan because the underlying scanner may mutate it.
      Scan scanClone = new Scan(source.read.serializableScan.get());
      scanner = table.getScanner(scanClone);
      iter = scanner.iterator();
      return advance();
    }

    @Override
    public Result getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public boolean advance() throws IOException {
      if (!iter.hasNext()) {
        return rangeTracker.markDone();
      }
      final Result next = iter.next();
      boolean hasRecord = rangeTracker.tryReturnRecordAt(true, ByteKey.copyFrom(next.getRow()))
          || rangeTracker.markDone();
      if (hasRecord) {
        current = next;
        ++recordsReturned;
      }
      return hasRecord;
    }

    @Override
    public void close() throws IOException {
      LOG.debug("Closing reader after reading {} records.", recordsReturned);
      if (scanner != null) {
        scanner.close();
        scanner = null;
      }
      if (connection != null) {
        connection.close();
        connection = null;
      }
    }

    @Override
    public synchronized AbstractSource getCurrentSource() {
      return source;
    }

    @Override
    public final Double getFractionConsumed() {
      return rangeTracker.getFractionConsumed();
    }

    @Override
    public final long getSplitPointsConsumed() {
      return rangeTracker.getSplitPointsConsumed();
    }

    @Override
    @Nullable
    public final synchronized AbstractSource splitAtFraction(double fraction) {
      ByteKey splitKey;
      try {
        splitKey = rangeTracker.getRange().interpolateKey(fraction);
      } catch (RuntimeException e) {
        LOG.info("{}: Failed to interpolate key for fraction {}.", rangeTracker.getRange(),
            fraction, e);
        return null;
      }

      LOG.info("Proposing to split {} at fraction {} (key {})", rangeTracker, fraction, splitKey);

      AbstractSource primary;
      AbstractSource residual;
      try {
        primary = source.withEndKey(splitKey);
        residual = source.withStartKey(splitKey);
      } catch (Exception e) {
        LOG.info("{}: Interpolating for fraction {} yielded invalid split key {}.",
            rangeTracker.getRange(), fraction, splitKey, e);
        return null;
      }
      if (!rangeTracker.trySplitAtPosition(splitKey)) {
        return null;
      }
      this.source = primary;
      return residual;
    }
  }

  /**
   * A abstract {@link PTransform} that writes to HBase. See the class-level Javadoc on implementations
   * {@link HBaseIO} and {@link HBaseBigtableIO} for more information.
   */
  protected abstract static class AbstractWrite extends PTransform<PCollection<Mutation>, PDone> {
    private static final long serialVersionUID = 0L;

    protected final ValueProvider<String> tableId;
    protected final SerializableConfiguration serializableConfiguration;

    protected AbstractWrite(SerializableConfiguration serializableConfiguration,
        ValueProvider<String> tableId) {
      this.serializableConfiguration = serializableConfiguration;
      this.tableId = tableId;
    }

    public ValueProvider<String> getTableId() {
      return tableId;
    }

    public Configuration getConfiguration() {
      return serializableConfiguration.get();
    }

    public void validate() {
      checkArgument(serializableConfiguration != null, "withConfiguration() is required");
      checkArgument(!tableId.isAccessible() || tableId.get() != null, "withTableId() is required");
    }

    @Override
    public PDone expand(PCollection<Mutation> input) {
      validate();

      try (Connection connection = ConnectionFactory
          .createConnection(serializableConfiguration.get())) {
        Admin admin = connection.getAdmin();
        checkArgument(admin.tableExists(TableName.valueOf(getTableId().get())),
            "Table %s does not exist", tableId);
      } catch (IOException e) {
        LOG.warn("Error checking whether table {} exists; proceeding.", tableId, e);
      }

      input.apply(ParDo.of(new HBaseWriterFn(tableId, serializableConfiguration)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("configuration", serializableConfiguration.get().toString()));
      builder.add(DisplayData.item("tableId", tableId));
    }

    private class HBaseWriterFn extends DoFn<Mutation, Void> {
      private static final long serialVersionUID = 0L;

      public HBaseWriterFn(ValueProvider<String> tableId,
          SerializableConfiguration serializableConfiguration) {
        this.tableId = checkNotNull(tableId, "tableId");
        this.serializableConfiguration = checkNotNull(serializableConfiguration,
            "serializableConfiguration");
      }

      @Setup
      public void setup() throws Exception {
        connection = ConnectionFactory.createConnection(serializableConfiguration.get());
      }

      @StartBundle
      public void startBundle(StartBundleContext c) throws IOException {
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableId.get()));
        mutator = connection.getBufferedMutator(params);
        recordsWritten = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        mutator.mutate(c.element());
        ++recordsWritten;
      }

      @FinishBundle
      public void finishBundle() throws Exception {
        mutator.flush();
        LOG.debug("Wrote {} records", recordsWritten);
      }

      @Teardown
      public void tearDown() throws Exception {
        if (mutator != null) {
          mutator.close();
          mutator = null;
        }
        if (connection != null) {
          connection.close();
          connection = null;
        }
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.delegate(AbstractWrite.this);
      }

      private final ValueProvider<String> tableId;
      private final SerializableConfiguration serializableConfiguration;

      private Connection connection;
      private BufferedMutator mutator;

      private long recordsWritten;
    }
  }
}
