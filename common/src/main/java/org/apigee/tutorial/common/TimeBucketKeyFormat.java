package org.apigee.tutorial.common;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;

import java.util.TimeZone;

/**
 * Utility to create date "buckets" for keys in common time-series insertions
 *
 * @author zznate
 */
enum TimeBucketKeyFormat {
  ALL("__ALL__"),
  MONTH("yyyy_MM"),
  WEEK("yyyy_MM_w"),
  DAY("yyyy_MM_dd"),
  HOUR("yyyy_MM_dd_HH"),
  MINUTE("yyyy_MM_dd_HH_mm"),
  SECOND("yyyy_MM_dd_HH_mm_ss");

  public static final String ALL_KEY = "__ALL__";

  FastDateFormat formatter;

  TimeBucketKeyFormat(String format) {
    if ( !StringUtils.equals(format, ALL_KEY)) {
      this.formatter = FastDateFormat.getInstance(format, TimeZone.getTimeZone("GMT"));
    }
  }

  /**
   * Return the formatterd date based on the type. In the case of __ALL__
   * we just return the format string, ignoring the date
   * @param date
   * @return
   */
  public String formatDate(long date) {
    if (this == TimeBucketKeyFormat.ALL ) {
      return toString();
    }
    return formatter.format(date);
  }

  @Override
  public String toString() {
    if ( formatter == null ) {
      return ALL_KEY;
    }
    return formatter.getPattern();
  }
}
