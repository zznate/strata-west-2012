package org.apigee.tutorial;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.math.RandomUtils;
import org.apigee.tutorial.common.SchemaUtils;
import org.apigee.tutorial.common.TutorialBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Inserts a million columns of randomly generated longs into a single row
 * with a TimeUUID key. Column names are timestamps with <b>micro</b>second precision.
 * This class uses 5 threads to run these inserts in parallel.
 *
 * #TIP:
 * To see how to handle creation of microsecond-resolution clocks in Java, see
 * {@link me.prettyprint.cassandra.service.clock.MicrosecondsSyncClockResolution} in hector-core.
 *
 * #TIP
 * Use this simple multi-threaded approach for inserts in any batch-loading scenarios.
 *
 * mvn -e exec:java -Dexec.mainClass="org.apigee.tutorial.TimeseriesInserter"
 * @author zznate
 */
public class TimeseriesInserter extends TutorialBase {
  private static Logger log = LoggerFactory.getLogger(TimeseriesInserter.class);

  private static ExecutorService exec;

  public static final String CF_TIMESERIES_SINGLE_ROW = "TimeseriesSingleRow";

  public static void main(String[] args) {
    init();
    maybeCreateSchema();
    long startTime = System.currentTimeMillis();
    init();
    exec = Executors.newFixedThreadPool(5);
    try {

      List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
      // Generate the TIME UUID key we will use for this row:
      // String myKey = TimeUUIDUtils.getTimeUUID(tutorialKeyspace.createClock()).toString();
      // we'll use a simple key for convenience though
      String myKey = "myKey";
      log.info("Generated key: {}", myKey);
      // request 200 invocation of RowInserter
      // each invocation creates 5000 columns, so we get a 1 million column "wide" row
      for ( int x=0; x<200; x++ ) {
        futures.add(exec.submit(new TimeseriesInserter()
                .new RowInserter(myKey)));
      }

      int total = 0;
      try {
        for ( Future<Integer> f : futures ) {
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
    tutorialCluster.getConnectionManager().shutdown();
  }

  protected static void maybeCreateSchema() {
    BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
    columnFamilyDefinition.setKeyspaceName(SchemaUtils.TUTORIAL_KEYSPACE_NAME);
    columnFamilyDefinition.setName(CF_TIMESERIES_SINGLE_ROW);
    columnFamilyDefinition.setComparatorType(ComparatorType.LONGTYPE);
    columnFamilyDefinition.setDefaultValidationClass(ComparatorType.LONGTYPE.getClassName());
    columnFamilyDefinition.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
    ColumnFamilyDefinition cfDef = new ThriftCfDef(columnFamilyDefinition);
    schemaUtils.maybeCreate(cfDef);
  }


  class RowInserter implements Callable<Integer> {

    private String myKey;

    RowInserter(String myKey) {
      this.myKey = myKey;
    }

    /**
     * Each invocation creates 20 rows of 50 keys each
     * @return
     */
    public Integer call() {
      Mutator<String> mutator = HFactory.createMutator(tutorialKeyspace, StringSerializer.get());
      int count = 0;

      for (int x=0; x<5000; x++) {
        mutator.addInsertion(myKey,CF_TIMESERIES_SINGLE_ROW, buildColumnFor(x));
      }
      mutator.execute();
      log.debug("Inserted {} rows", count);
      return Integer.valueOf(count);
    }
  }

  private HColumn<Long,Long> buildColumnFor(int colName) {
    // Using the clock available through hector to generate microsecond precision longs
    HColumn<Long,Long> column = HFactory.createColumn(tutorialKeyspace.createClock(), RandomUtils.nextLong(),
      LongSerializer.get(), LongSerializer.get());
    return column;
  }

}
