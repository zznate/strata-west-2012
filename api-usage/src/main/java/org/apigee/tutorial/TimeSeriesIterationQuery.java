package org.apigee.tutorial;

import com.eaio.uuid.UUID;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;
import org.apache.commons.lang.StringUtils;
import org.apigee.tutorial.common.SchemaUtils;
import org.apigee.tutorial.common.TutorialBase;
import org.apigee.tutorial.common.TutorialUsageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;


/**
 * Iterate over the columns of the TimeseriesSingleRow column
 * family. This demonstrates one of the most common types of
 * queries, a retrieving a 'slice' of columns within a row. This
 * class uses {@link me.prettyprint.cassandra.service.ColumnSliceIterator}
 * from hector to handle paging over the columns in batches of 100.
 *
 * This class also takes arguments to modify the size of the batch. Though
 * you can request a slice of 1 million columns as a legitimate API call, give
 * it a try through this class to get an understanding of why that is a
 * "bad idea".
 *
 * There are several ways to observe the behavior of Cassandra while modifying
 * workloads:
 *
 * Use jconsole attached to the Cassandra process to see the memory
 * usage changes.
 *
 * DataStax's OpsCenter tool
 * - watch latency spike and potential node health changes
 *
 * #TIP
 * Modify this class as needed to narrow down on the most efficient batch sizes
 * for your specific data model and workload. Start at 500 and go up or down
 * as needed.
 *
 * #NOTE
 * Storing rows this wide in production can produce "hot spots" depending on the
 * read patterns. See the BucketingTimeseriesQuery example in the storage-model
 * section of this tutorial.
 *
 * #CQL
 * A CQL query which would retrieve the first 100 columns for this row. This currently
 * only shows the key pending the implementation of of composite column support in CQL
 * select * from CompositeSingleRowIndex where KEY = 'ALL';
 *
 * A CQL query to retrieve columns 1001-2000:
 * TODO
 *
 * A CQL query to retrieve all columns in 1 call
 * TODO
 *
 * Returning all columns from the cassandra-cli
 *
 *
 * mvn -e exec:java -Dexec.mainClass="org.apigee.tutorial.TimeseriesIterationQuery"
 * @author zznate
 */
public class TimeseriesIterationQuery extends TutorialBase {
  private static Logger log = LoggerFactory.getLogger(TimeseriesIterationQuery.class);

  // ColumnSliceIterator with paging
  public static void main(String [] args) {
    init();
    verifySchema();
    String key = "myKey";
    long timer = System.currentTimeMillis();
    TimeseriesIterationQuery tiq = new TimeseriesIterationQuery();
    tiq.printColumns(key);
    log.info("Iteration took {} seconds", (System.currentTimeMillis() - timer) / 1000);
  }

  public void printColumns(String key) {
    TimeseriesIterator ti = new TimeseriesIterator(key);
    int count = 0;
    for (HColumn<Long,Long> column : ti ) {
      log.info("Column name: {} Column Value: {}", column.getName(), column.getValue());
      count++;
    }
    log.info("Read a total of {} columns", count);
  }


  protected static void verifySchema() {
    if ( !schemaUtils.cfExists(TimeseriesInserter.CF_TIMESERIES_SINGLE_ROW) ) {
      throw new TutorialUsageException("Please run TimeseriesInserter to generate the required data for this example.");
    }
  }

  /**
   * An iterator implementation is a clean way to pass back up to the caller
   * the concept of seemlessly scanning a wide row in an efficient manner.
   */
  class TimeseriesIterator implements Iterable<HColumn<Long,Long>> {

    private ColumnSliceIterator<String,Long,Long> sliceIterator;

    TimeseriesIterator(String key) {
      SliceQuery<String,Long,Long> sliceQuery =
              HFactory.createSliceQuery(tutorialKeyspace, StringSerializer.get(), LongSerializer.get(), LongSerializer.get());
      sliceQuery.setColumnFamily(TimeseriesInserter.CF_TIMESERIES_SINGLE_ROW);
      sliceQuery.setKey(key);

      sliceIterator = new ColumnSliceIterator<String,Long,Long>(sliceQuery,0L,Long.MAX_VALUE,false);
    }


    @Override
    public Iterator<HColumn<Long, Long>> iterator() {
      return sliceIterator;
    }
  }
}
