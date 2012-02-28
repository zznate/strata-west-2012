package org.apigee.tutorial.common;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Utility to create date "buckets" for keys in common time-series insertions
 *
 * @author zznate
 */
public enum TimeBucketKeyFormat {
  ALL("__ALL__"),
  MONTH("yyyy_MM"),
  WEEK("yyyy_MM_w"),
  DAY("yyyy_MM_dd"),
  HOUR("yyyy_MM_dd_HH"),
  MINUTE("yyyy_MM_dd_HH_mm"),
  SECOND("yyyy_MM_dd_HH_mm_ss");

  public static final String ALL_KEY = "__ALL__";

  FastDateFormat formatter;
  final String format;

  TimeBucketKeyFormat(String format) {
    this.format = format;
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

  /**
   * Return the long value representing this bucket key
   * @param bucketKey
   * @return
   */
  public long parse(String bucketKey) {
    if ( bucketKey.equals(ALL_KEY) ) {
      throw new IllegalArgumentException("Can't parse key for " + ALL_KEY);
    }
    try {
      SimpleDateFormat sdf = new SimpleDateFormat(format);
      return sdf.parse(bucketKey).getTime();
    } catch (Exception e) {
      throw new IllegalArgumentException("could not parse bucket key: " + bucketKey);
    }
  }

  @Override
  public String toString() {
    if ( formatter == null ) {
      return ALL_KEY;
    }
    return formatter.getPattern();
  }
}
