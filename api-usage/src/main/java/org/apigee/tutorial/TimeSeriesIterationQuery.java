package org.apigee.tutorial;

import org.apigee.tutorial.common.SchemaUtils;
import org.apigee.tutorial.common.TutorialBase;
import org.apigee.tutorial.common.TutorialUsageException;

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
 * A CQL query which would retrieve the first 100 columns for this row:
 * TODO
 *
 * A CQL query to retrieve columns 1001-2000:
 * TODO
 *
 * A CQL query to retrieve all columns in 1 call
 *
 * @author zznate
 */
public class TimeSeriesIterationQuery extends TutorialBase {

  // ColumnSliceIterator with paging


  @Override
  protected void maybeCreateSchema() {
    if ( !schemaUtils.cfExists(TimeseriesInserter.CF_TIMESERIES_SINGLE_ROW) ) {
      throw new TutorialUsageException("Please run TimeseriesInserter to generate the required data for this example.");
    }
  }
}
