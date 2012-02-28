package org.apigee.tutorial;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apigee.tutorial.common.SchemaUtils;
import org.apigee.tutorial.common.TimeBucketKeyFormat;
import org.apigee.tutorial.common.TutorialBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Bucket the dates to make rows more manageable. Because the row is the unit
 * of partition in a Cassandra cluster, this makes it easier to distributed the
 * load evenly accross the cluster.
 *
 * #TIP
 * See the {@link org.apigee.tutorial.common.TimeBucketKeyFormat}
 *
 * mvn -e exec:java -Dexec.mainClass="org.apigee.tutorial.BucketingTimeSeriesInserter"
 *
 * @author zznate
 */
public class BucketingTimeSeriesInserter extends TutorialBase {
  private static Logger log = LoggerFactory.getLogger(BucketingTimeSeriesInserter.class);

  public static final String CF_BUCKETED_TIMSERIES = "BucketedTimeSeries";

  private static ExecutorService exec;

  public static void main(String[] args) {
    init();
    maybeCreateSchema();
    exec = Executors.newFixedThreadPool(3);
    try {

      List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
      List<String> keys = buildKeys();
      List<String> subKeys = new ArrayList<String>(1000);
      for (int x=0; x<keys.size();x++) {
        subKeys.add(keys.get(x));
        if ( x % 500 == 0 ) {
          futures.add(exec.submit(new BucketingTimeSeriesInserter().new BucketingRowInserter(subKeys)));
          subKeys = new ArrayList(500);
          log.info("Submitting 500 keys");
        }
      }

      int total = 0;
      try {
        for (Future<Integer> f : futures) {
          total += f.get().intValue();
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      }

    } catch (Exception e) {
      log.error("Could not locate file",e);
    } finally {
      exec.shutdown();
    }
  }


  protected static void maybeCreateSchema() {
    BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
    columnFamilyDefinition.setKeyspaceName(SchemaUtils.TUTORIAL_KEYSPACE_NAME);
    columnFamilyDefinition.setName(CF_BUCKETED_TIMSERIES);
    columnFamilyDefinition.setComparatorType(ComparatorType.LONGTYPE);
    columnFamilyDefinition.setDefaultValidationClass(ComparatorType.LONGTYPE.getClassName());
    columnFamilyDefinition.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
    ColumnFamilyDefinition cfDef = new ThriftCfDef(columnFamilyDefinition);
    schemaUtils.maybeCreate(cfDef);
  }

  class BucketingRowInserter implements Callable {

    private List<String> keys;

    BucketingRowInserter(List<String> keys) {
      this.keys = keys;
    }

    @Override
    public Object call() throws Exception {
      // TODO for key : keys,
      Mutator<String> mutator = HFactory.createMutator(tutorialKeyspace, StringSerializer.get());
      List<Long> colNames;
      int count = 0;
      log.info("building for key size {}", keys.size());
      for ( String key : keys ) {

        colNames = buildTimestampRangeFromKey(key);
        for ( Long colName : colNames ) {
          mutator.addInsertion(key, CF_BUCKETED_TIMSERIES, buildColumnFor(colName));
          if ( count % 1000 == 0 ) {
            mutator.execute();
            log.info("sending mutation count{} ", count);
          }
          count++;
        }

      }
      mutator.execute();

      // - build timerangefromkey
      // for every 1000 cols, insert
      return keys.size();
    }

    private HColumn<Long,Long> buildColumnFor(Long colName) {
        HColumn<Long,Long> column = HFactory.createColumn(tutorialKeyspace.createClock(), RandomUtils.nextLong(),
                LongSerializer.get(), LongSerializer.get());
        return column;
      }

  }

  /**
   * Build the keys for every minute of the past 7 days
   * @return
   */
  public static List<String> buildKeys() {
    int oneMonthInMinutes = 60 * 24 * 1;
    List<String> keys = new ArrayList<String>(oneMonthInMinutes);
    TimeBucketKeyFormat minutes = TimeBucketKeyFormat.MINUTE;
    long now = System.currentTimeMillis();
    for (int x=1; x<=oneMonthInMinutes; x++) {
      keys.add(minutes.formatDate(now - (x * 60 * 1000)));
    }
    return keys;
  }

  /**
   * Build a range of timestamp column from a given key
   * @param key
   * @return
   */
  private List<Long> buildTimestampRangeFromKey(String key) {
    long keyTime = format.parse(key);
    List<Long> cols = new ArrayList<Long>(6000);
    Random random = new Random();
    // create 6,000 columns
    for(int x=0; x< 60*1000; x++) {
      if ( x % 10 == 0 ) {
        cols.add(keyTime + x + random.nextInt(10));
      }
    }
    return cols;
  }

  static TimeBucketKeyFormat format = TimeBucketKeyFormat.MINUTE;
}
