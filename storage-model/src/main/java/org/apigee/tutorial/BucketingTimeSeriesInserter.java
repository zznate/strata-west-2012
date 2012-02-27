package org.apigee.tutorial;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apigee.tutorial.common.SchemaUtils;
import org.apigee.tutorial.common.TimeBucketKeyFormat;
import org.apigee.tutorial.common.TutorialBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * Bucket the dates to make rows more manageable. Because the row is the unit
 * of partition in a Cassandra cluster, this makes it easier to distributed the
 * load evenly accross the cluster.
 *
 * #TIP
 * See the {@link org.apigee.tutorial.common.TimeBucketKeyFormat}
 *
 * @author zznate
 */
public class BucketingTimeSeriesInserter extends TutorialBase {

  public static final String CF_BUCKETED_TIMSERIES = "BucketedTimeSeries";

  public static void main(String[] args) {
    // create executor
    // TODO executor(int tpSize) on TutorialBase
  }

  @Override
  protected void maybeCreateSchema() {
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
    @Override
    public Object call() throws Exception {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

  }

  /**
   * Build the keys for every minute of the past 31 days
   * @return
   */
  public static List<String> buildKeys() {
    int oneMonthInMinutes = 60 * 24 * 31;
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
    long keyTime = TimeBucketKeyFormat.MINUTE.parse(key);
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
}
