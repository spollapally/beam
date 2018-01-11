package org.apache.beam.sdk.io.gcp.hbasebigtable;

import static org.apache.beam.sdk.testing.SourceTestUtils.assertSourcesEqualReferenceSource;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionExhaustive;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionFails;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.hbasebigtable.HBaseBigtableIO.BigtableSource;
import org.apache.beam.sdk.io.hbase.HBaseMutationCoder;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;

/**
 * HBaseBigtableIOTest integration tests
 */
@RunWith(JUnit4.class)
public class HBaseBigtableIOTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();
  
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
  private static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("cq");
  private static final byte[] COLUMN_EMAIL = Bytes.toBytes("email");

  private static String projectId = "sduskis-hello-shakespear";
  private static String instanceId = "beam-test";

  private static String tableId;
  private static Connection bigtableConn;
  private static Admin admin;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = BigtableConfiguration.configure(projectId, instanceId);
    bigtableConn = ConnectionFactory.createConnection(conf);
    admin = bigtableConn.getAdmin();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (bigtableConn != null) {
      bigtableConn.close();
      bigtableConn = null;
      admin = null;
    }
  }

  @Before
  public void setup() throws Exception {
    tableId = String.format("BigtableIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date());
  }

  @After
  public void tearDown() throws Exception {
    deleteTable(tableId);
  }

  @Test
  public void testReadBuildsCorrectly() {
    HBaseBigtableIO.Read read = HBaseBigtableIO.read().withProjectId(projectId)
        .withInstanceId(instanceId).withTableId(tableId);
    assertEquals(tableId, read.getTableId().get());
    assertNotNull("configuration", read.getConfiguration());
    assertNotNull(projectId, read.getProjectId());
    assertNotNull(instanceId, read.getInstanceId());
  }

  @Test
  public void testReadBuildsCorrectlyInDifferentOrder() {
    HBaseBigtableIO.Read read = HBaseBigtableIO.read().withTableId(tableId)
        .withInstanceId(instanceId).withProjectId(projectId);
    assertEquals(tableId, read.getTableId().get());
    assertNotNull("configuration", read.getConfiguration());
    assertNotNull(projectId, read.getProjectId());
    assertNotNull(instanceId, read.getInstanceId());
  }

  @Test
  public void testWriteBuildsCorrectly() {
    HBaseBigtableIO.Write write = HBaseBigtableIO.write().withProjectId(projectId)
        .withInstanceId(instanceId).withTableId(tableId);
    assertEquals(tableId, write.getTableId().get());
    assertNotNull("configuration", write.getConfiguration());
    assertNotNull(projectId, write.getProjectId());
    assertNotNull(instanceId, write.getInstanceId());
  }

  @Test
  public void testWriteBuildsCorrectlyInDifferentOrder() {
    HBaseBigtableIO.Write write = HBaseBigtableIO.write().withTableId(tableId)
        .withInstanceId(instanceId).withProjectId(projectId);
    assertEquals(tableId, write.getTableId().get());
    assertNotNull("configuration", write.getConfiguration());
    assertNotNull(projectId, write.getProjectId());
    assertNotNull(instanceId, write.getInstanceId());
  }

  @Test
  public void testWriteValidationFailsMissingTable() {
    HBaseBigtableIO.Write write = HBaseBigtableIO.write().withInstanceId(instanceId).withProjectId(projectId);
    thrown.expect(IllegalArgumentException.class);
    write.expand(null /* input */);
  }

  @Test
  public void testWriteValidationFailsMissingProject() {
    HBaseBigtableIO.Write write = HBaseBigtableIO.write().withInstanceId(instanceId).withTableId(tableId);
    thrown.expect(IllegalArgumentException.class);
    write.expand(null /* input */);
  }

  @Test
  public void testWriteValidationFailsMissingInstance() {
    HBaseBigtableIO.Write write = HBaseBigtableIO.write().withProjectId(projectId).withTableId(tableId);
    thrown.expect(IllegalArgumentException.class);
    write.expand(null /* input */);
  }

  @Test
  public void testReadingFailsTableDoesNotExist() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Table %s does not exist", tableId));
    runReadTest(HBaseBigtableIO.read().withProjectId(projectId).withInstanceId(instanceId)
        .withTableId(tableId), new ArrayList<Result>());
  }

  //TODO - add test cases for withConfiguration
  
  @Test
  public void testReadingEmptyTable() throws Exception {
    createTable(tableId);
    runReadTest(HBaseBigtableIO.read().withProjectId(projectId).withInstanceId(instanceId)
        .withTableId(tableId), new ArrayList<Result>());
  }

  @Test
  public void testReading() throws Exception {
    final int numRows = 1001;
    createTable(tableId);
    writeData(tableId, numRows);
    runReadTestLength(HBaseBigtableIO.read().withProjectId(projectId).withInstanceId(instanceId)
        .withTableId(tableId), numRows);
  }

  @Test
  public void testReadingWithSplits() throws Exception {
    final int numRows = 1500;
    final int numRegions = 4;
    final long bytesPerRow = 100L;

    // Set up test table data and sample row keys for size estimation and splitting.
    createTable(tableId);
    writeData(tableId, numRows);

    HBaseBigtableIO.Read read = HBaseBigtableIO.read().withProjectId(projectId)
        .withInstanceId(instanceId).withTableId(tableId);
    BigtableSource source = new BigtableSource(read, null /* estimatedSizeBytes */);
    List<? extends BoundedSource<Result>> splits = source.split(numRows * bytesPerRow / numRegions,
        null /* options */);

    // Test num splits and split equality.
    assertThat(splits, hasSize(4));
    assertSourcesEqualReferenceSource(source, splits, null /* options */);
  }

  @Test
  public void testReadingSourceTwice() throws Exception {
    final int numRows = 10;

    // Set up test table data and sample row keys for size estimation and splitting.
    createTable(tableId);
    writeData(tableId, numRows);

    HBaseBigtableIO.Read read = HBaseBigtableIO.read().withProjectId(projectId)
        .withInstanceId(instanceId).withTableId(tableId);
    BigtableSource source = new BigtableSource(read, null /* estimatedSizeBytes */);
    assertThat(SourceTestUtils.readFromSource(source, null), hasSize(numRows));
    // second read.
    assertThat(SourceTestUtils.readFromSource(source, null), hasSize(numRows));
  }

  /** Tests reading all rows using a filter. */
  @Test
  public void testReadingWithFilter() throws Exception {
    final int numRows = 1001;

    createTable(tableId);
    writeData(tableId, numRows);

    String regex = ".*17.*";
    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    HBaseBigtableIO.Read read = HBaseBigtableIO.read().withProjectId(projectId)
        .withInstanceId(instanceId).withTableId(tableId).withFilter(filter);

    runReadTestLength(read, 20);
  }

  /**
   * Tests reading all rows using key ranges. Tests a prefix [), a suffix (], and a restricted range
   * [] and that some properties hold across them.
   */
  @Test
  public void testReadingWithKeyRange() throws Exception {
    final int numRows = 1001;
    final byte[] startRow = "2".getBytes();
    final byte[] stopRow = "9".getBytes();
    final ByteKey startKey = ByteKey.copyFrom(startRow);

    createTable(tableId);
    writeData(tableId, numRows);

    // Test prefix: [beginning, startKey).
    final ByteKeyRange prefixRange = ByteKeyRange.ALL_KEYS.withEndKey(startKey);
    runReadTestLength(
        HBaseBigtableIO.read().withProjectId(projectId)
        .withInstanceId(instanceId).withTableId(tableId).withKeyRange(prefixRange), 126);

    // Test suffix: [startKey, end).
    final ByteKeyRange suffixRange = ByteKeyRange.ALL_KEYS.withStartKey(startKey);
    runReadTestLength(
        HBaseBigtableIO.read().withProjectId(projectId)
        .withInstanceId(instanceId).withTableId(tableId).withKeyRange(suffixRange), 875);

    // Test restricted range: [startKey, endKey).
    // This one tests the second signature of .withKeyRange
    runReadTestLength(
        HBaseBigtableIO.read().withProjectId(projectId)
        .withInstanceId(instanceId).withTableId(tableId).withKeyRange(startRow, stopRow),
        441);
  }

  /** Tests d/ynamic work rebalancing exhaustively. */
  //@Test -- takes a long time
  public void testReadingSplitAtFractionExhaustive() throws Exception {
    final int numRows = 7;

    createTable(tableId);
    writeData(tableId, numRows);

    HBaseBigtableIO.Read read = HBaseBigtableIO.read().withProjectId(projectId)
        .withInstanceId(instanceId).withTableId(tableId);
    BigtableSource source = new BigtableSource(read, null /* estimatedSizeBytes */)
        .withStartKey(ByteKey.of(48))
        .withEndKey(ByteKey.of(58));

    assertSplitAtFractionExhaustive(source, null);
  }

  /** Unit tests of splitAtFraction. */
  @Test
  public void testReadingSplitAtFraction() throws Exception {
    final int numRows = 10;

    createTable(tableId);
    writeData(tableId, numRows);

    HBaseBigtableIO.Read read = HBaseBigtableIO.read().withProjectId(projectId)
        .withInstanceId(instanceId).withTableId(tableId);
    BigtableSource source = new BigtableSource(read, null /* estimatedSizeBytes */);

    // The value k is based on the partitioning schema for the data, in this test case,
    // the partitioning is HEX-based, so we start from 1/16m and the value k will be
    // around 1/256, so the tests are done in approximately k ~= 0.003922 steps
    double k = 0.003922;

    assertSplitAtFractionFails(source, 0, k, null /* options */);
    assertSplitAtFractionFails(source, 0, 1.0, null /* options */);
    // With 1 items read, all split requests past k will succeed.
    assertSplitAtFractionSucceedsAndConsistent(source, 1, k, null /* options */);
    assertSplitAtFractionSucceedsAndConsistent(source, 1, 0.666, null /* options */);
    // With 3 items read, all split requests past 3k will succeed.
    assertSplitAtFractionFails(source, 3, 2 * k, null /* options */);
    assertSplitAtFractionSucceedsAndConsistent(source, 3, 3 * k, null /* options */);
    assertSplitAtFractionSucceedsAndConsistent(source, 3, 4 * k, null /* options */);
    // With 6 items read, all split requests past 6k will succeed.
    assertSplitAtFractionFails(source, 6, 5 * k, null /* options */);
    assertSplitAtFractionSucceedsAndConsistent(source, 6, 0.7, null /* options */);
  }

  @Test
  public void testReadingDisplayData() {
    HBaseBigtableIO.Read read = HBaseBigtableIO.read().withProjectId(projectId)
        .withInstanceId(instanceId).withTableId(tableId);
    DisplayData displayData = DisplayData.from(read);
    assertThat(displayData, hasDisplayItem("tableId", tableId));
    assertThat(displayData, hasDisplayItem("configuration"));
    assertThat(displayData, hasDisplayItem("projectId", projectId));
    assertThat(displayData, hasDisplayItem("instanceId", instanceId));
  }

  @Test
  public void testWriting() throws Exception {
    final String key = "key";
    final String value = "value";
    final int numMutations = 100;

    createTable(tableId);

    p.apply("multiple rows", Create.of(makeMutations(key, value, numMutations))).apply("write",
        HBaseBigtableIO.write().withProjectId(projectId).withInstanceId(instanceId)
            .withTableId(tableId));
    p.run().waitUntilFinish();

    List<Result> results = readTable(tableId, new Scan());
    assertEquals(numMutations, results.size());
  }

  @Test
  public void testWritingFailsTableDoesNotExist() throws Exception {
    // Exception will be thrown by write.expand() when writeToDynamic is applied.
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Table %s does not exist", tableId));
    p.apply(Create.empty(HBaseMutationCoder.of()))
        .apply("write", HBaseBigtableIO.write().withProjectId(projectId).withInstanceId(instanceId)
            .withTableId(tableId));
  }

  /** Tests that when writing an element fails, the write fails. */
  @Test
  public void testWritingFailsBadElement() throws Exception {
    final String key = "KEY";
    createTable(tableId);

    p.apply(Create.of(makeBadMutation(key)))
        .apply(HBaseBigtableIO.write().withProjectId(projectId).withInstanceId(instanceId)
            .withTableId(tableId));

    thrown.expect(Pipeline.PipelineExecutionException.class);
    thrown.expectCause(Matchers.<Throwable>instanceOf(IllegalArgumentException.class));
    thrown.expectMessage("No columns to insert");
    p.run().waitUntilFinish();
    
    //TODO - fix test case
    // fails with org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException instead of PipelineExecutionException
  }

  @Test
  public void testWritingDisplayData() {
    HBaseBigtableIO.Write write = HBaseBigtableIO.write().withProjectId(projectId).withInstanceId(instanceId)
        .withTableId(tableId);
    DisplayData displayData = DisplayData.from(write);
    assertThat(displayData, hasDisplayItem("tableId", tableId));
    assertThat(displayData, hasDisplayItem("projectId", projectId));
    assertThat(displayData, hasDisplayItem("instanceId", instanceId));
  }

  /** Helper methods **/
  private void createTable(String tableId) throws Exception {
    byte[][] splitKeys = { "4".getBytes(), "8".getBytes(), "C".getBytes() };
    createTable(tableId, COLUMN_FAMILY, splitKeys);
  }

  private void createTable(String tableId, byte[] columnFamily, byte[][] splitKeys)
      throws Exception {
    TableName tableName = TableName.valueOf(tableId);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor colDef = new HColumnDescriptor(columnFamily);
    desc.addFamily(colDef);
    admin.createTable(desc, splitKeys);
  }

  private void deleteTable(String tableId) throws IOException {
    TableName tableName = TableName.valueOf(tableId);
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  private void writeData(String tableId, int numRows) throws Exception {
    Connection connection = admin.getConnection();
    TableName tableName = TableName.valueOf(tableId);
    BufferedMutator mutator = connection.getBufferedMutator(tableName);
    List<Mutation> mutations = makeTableData(numRows);
    mutator.mutate(mutations);
    mutator.flush();
    mutator.close();
  }

  private static List<Mutation> makeTableData(int numRows) {
    List<Mutation> mutations = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; ++i) {
      // We pad values in hex order 0,1, ... ,F,0, ...
      String prefix = String.format("%X", i % 16);
      // This 21 is to have a key longer than an input
      byte[] rowKey = Bytes.toBytes(StringUtils.leftPad("_" + String.valueOf(i), 21, prefix));
      byte[] value = Bytes.toBytes(String.valueOf(i));
      byte[] valueEmail = Bytes.toBytes(String.valueOf(i) + "@email.com");
      mutations.add(new Put(rowKey).addColumn(COLUMN_FAMILY, COLUMN_QUALIFIER, value));
      mutations.add(new Put(rowKey).addColumn(COLUMN_FAMILY, COLUMN_EMAIL, valueEmail));
    }
    return mutations;
  }

  private Iterable<Mutation> makeMutations(String key, String value, int numMutations) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < numMutations; i++) {
      mutations.add(makeMutation(key + i, value));
    }
    return mutations;
  }

  private Mutation makeMutation(String key, String value) {
    return new Put(key.getBytes(StandardCharsets.UTF_8))
        .addColumn(COLUMN_FAMILY, COLUMN_QUALIFIER, Bytes.toBytes(value))
        .addColumn(COLUMN_FAMILY, COLUMN_EMAIL, Bytes.toBytes(value + "@email.com"));
  }

  private static Mutation makeBadMutation(String key) {
    return new Put(key.getBytes());
  }

  private List<Result> readTable(String tableId, Scan scan) throws Exception {
    ResultScanner scanner = bigtableConn.getTable(TableName.valueOf(tableId)).getScanner(scan);
    List<Result> results = new ArrayList<>();
    for (Result result : scanner) {
      results.add(result);
    }
    scanner.close();
    return results;
  }

  private void runReadTest(HBaseBigtableIO.Read read, List<Result> expected) {
    final String transformId = read.getTableId() + "_" + read.getKeyRange();
    PCollection<Result> rows = p.apply("Read" + transformId, read);
    PAssert.that(rows).containsInAnyOrder(expected);
    p.run().waitUntilFinish();
  }

  private void runReadTestLength(HBaseBigtableIO.Read read, long numElements) {
    final String transformId = read.getTableId() + "_" + read.getKeyRange();
    PCollection<Result> rows = p.apply("Read" + transformId, read);
    PAssert.thatSingleton(rows.apply("Count" + transformId, Count.<Result>globally()))
        .isEqualTo(numElements);
    p.run().waitUntilFinish();
  }
}
