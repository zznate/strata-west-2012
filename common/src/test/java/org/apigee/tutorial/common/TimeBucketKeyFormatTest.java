package org.apigee.tutorial.common;


import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * Unit test for simple App.
 */
public class TimeBucketKeyFormatTest {

  public static final long TST_DATE = 1330104961369L;

  @Test
  public void testFormatDate() {
    TimeBucketKeyFormat format = TimeBucketKeyFormat.MONTH;
    String date = format.formatDate(TST_DATE);
    assertEquals("2012_02",date);

    format = TimeBucketKeyFormat.DAY;
    date = format.formatDate(TST_DATE);
    assertEquals("2012_02_24",date);

    format = TimeBucketKeyFormat.HOUR;
    date = format.formatDate(TST_DATE);
    assertEquals("2012_02_24_17",date);

    format = TimeBucketKeyFormat.MINUTE;
    date = format.formatDate(TST_DATE);
    assertEquals("2012_02_24_17_36",date);

    format = TimeBucketKeyFormat.SECOND;
    date = format.formatDate(TST_DATE);
    assertEquals("2012_02_24_17_36_01",date);
  }

  @Test
  public void testParse() {
    TimeBucketKeyFormat format = TimeBucketKeyFormat.MINUTE;
    format.parse("2012_02_28_01_09");
  }
}
