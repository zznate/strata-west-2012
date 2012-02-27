package org.apigee.tutorial;

import org.apigee.tutorial.common.TutorialBase;
import org.apigee.tutorial.common.TutorialUsageException;

/**
 * @author zznate
 */
public class BucketingTimeSeriesQuery extends TutorialBase {

  @Override
  protected void maybeCreateSchema() {
    if (!schemaUtils.cfExists(BucketingTimeSeriesInserter.CF_BUCKETED_TIMSERIES)) {
      throw new TutorialUsageException("You must first run BucketingTimeSeriesInserter before using this tutorial.");
    }
  }


}
