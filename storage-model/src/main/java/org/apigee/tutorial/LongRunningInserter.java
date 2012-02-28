package org.apigee.tutorial;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
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
import java.util.Random;
import java.util.concurrent.*;

/**
 * Low-intensity, long-running inserts. Use in combination with command-line
 * or monitoring systems such as DataStax's OpsCenter, Munin, Ganglia or similar.
 *
 * Defaults to 1 thread with a 10 second sleep between 1000 inserts. These parameters
 * are adjustable so you can test thresholds of a simple workload.
 *
 * Various was to see what is happening:
 * Column Family healt and workload statistics via 'nodetool' CLI:
 * ./bin/nodetool -h localhost cfstats
 *
 * To force some flushing, you can invoke flush directy:
 * ./bin/nodetool -h localhost flush TutorialKeyspace LongRunningInsert
 *
 * Thread pool usage statistics also via 'nodetool' CLI:
 * ./bin/nodetool -h localhost tpstats
 *
 * DataStax OpsCenter:
 * TODO add screen instructions
 *
 * mvn -e exec:java -Dexec.mainClass="org.apigee.tutorial.LongRunningInserter"
 * @author zznate
 */
public class LongRunningInserter extends TutorialBase {
  private static Logger log = LoggerFactory.getLogger(LongRunningInserter.class);

  public static final String CF_LONG_RUNNING_INSERT = "LongRunningInsert";

  private static ExecutorService exec;
  private static Random random = new Random();

  public static void main(String[] args ) {
    init();
    maybeCreateSchema();
    long startTime = System.currentTimeMillis();
    exec = Executors.newFixedThreadPool(3);

    try {

      List<Future<Integer>> futures = new ArrayList<Future<Integer>>();

      for (int x = 0; x < 200; x++) {
        futures.add(exec.submit(new LongRunningInserter().new RowInserter()));
        TimeUnit.SECONDS.sleep(random.nextInt(4));
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
    columnFamilyDefinition.setName(CF_LONG_RUNNING_INSERT);
    columnFamilyDefinition.setComparatorType(ComparatorType.LONGTYPE);
    columnFamilyDefinition.setDefaultValidationClass(ComparatorType.LONGTYPE.getClassName());
    columnFamilyDefinition.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
    ColumnFamilyDefinition cfDef = new ThriftCfDef(columnFamilyDefinition);
    schemaUtils.maybeCreate(cfDef);
  }


  class RowInserter implements Callable {

    @Override
    public Object call() throws Exception {
      Mutator<String> mutator = HFactory.createMutator(tutorialKeyspace, StringSerializer.get());
      int count = 0;
      String myKey = TimeUUIDUtils.getTimeUUID(tutorialKeyspace.createClock()).toString();
      for (int x=0; x<5000; x++) {

        // assemble the insertions
        mutator.addInsertion(myKey,CF_LONG_RUNNING_INSERT, buildColumnFor(x));
        if ( x % 1000 == 0 ) {
          myKey = TimeUUIDUtils.getTimeUUID(tutorialKeyspace.createClock()).toString();
          count++;
          TimeUnit.SECONDS.sleep(random.nextInt(4));
        }

      }
      mutator.execute();

      return Integer.valueOf(count);
    }
  }

  private HColumn<Long,Long> buildColumnFor(int colName) {
    HColumn<Long,Long> column = HFactory.createColumn(tutorialKeyspace.createClock(), RandomUtils.nextLong(),
      LongSerializer.get(), LongSerializer.get());
    return column;
  }
}
