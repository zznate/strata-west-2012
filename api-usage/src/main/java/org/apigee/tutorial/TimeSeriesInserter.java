package org.apigee.tutorial;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.commons.lang.math.RandomUtils;
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
 * Inserts 50 rows of data each with 1000 columns.
 * 
 * mvn -e exec:java -Dexec.mainClass="com.apigee.training.tutorial.ts.TimeseriesInserter"
 * @author zznate
 */
public class TimeseriesInserter {
  private static Logger log = LoggerFactory.getLogger(TimeseriesInserter.class);

  static Cluster trainingCluster;
  static Keyspace trainingKeyspace;
  static Properties properties;

  private static ExecutorService exec;

  public static void main(String[] args) {
    long startTime = System.currentTimeMillis();
    init();
    exec = Executors.newFixedThreadPool(5);
    try {

      List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

      // request 10 invocation of RowInserter
      // each invocation creates 5 rows, thus we have 50 rows of 1000 columns each
      for ( int x=0; x<10; x++ ) {
        futures.add(exec.submit(new TimeseriesInserter().new RowInserter()));
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
    trainingCluster.getConnectionManager().shutdown();
  }



  protected static void init() {
    // To modify the default ConsistencyLevel of QUORUM, create a
    // me.prettyprint.hector.api.ConsistencyLevelPolicy and use the overloaded form:
    // HFactory.createKeyspace("Composites", trainingCluster, consistencyLevelPolicy);
    // see also me.prettyprint.tutorial.model.ConfigurableConsistencyLevelPolicy[Test] for details

    trainingCluster = HFactory.getOrCreateCluster("TrainingCluster","127.0.0.1:9160");
    ConfigurableConsistencyLevel ccl = new ConfigurableConsistencyLevel();
    ccl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
    trainingKeyspace = HFactory.createKeyspace("TimeSeries", trainingCluster, ccl);
  }

    class RowInserter implements Callable<Integer> {

    /**
     * Each invocation creates 20 rows of 50 keys each
     * @return
     */
    public Integer call() {
      Mutator<String> mutator = HFactory.createMutator(trainingKeyspace, StringSerializer.get());
      int count = 0;
      String myKey = TimeUUIDUtils.getTimeUUID(trainingKeyspace.createClock()).toString();
      for (int x=0; x<5000; x++) {

        // assemble the insertions
        mutator.addInsertion(myKey,"Series1", buildColumnFor(x));
        if ( x % 1000 == 0 ) {
          myKey = TimeUUIDUtils.getTimeUUID(trainingKeyspace.createClock()).toString();
          count++;
        }

      }
      mutator.execute();
      log.debug("Inserted {} rows", count);
      return Integer.valueOf(count);
    }
  }

  private HColumn<Long,Long> buildColumnFor(int colName) {
    HColumn<Long,Long> column = HFactory.createColumn(trainingKeyspace.createClock(), RandomUtils.nextLong(),
      LongSerializer.get(), LongSerializer.get());
    return column;
  }

}
