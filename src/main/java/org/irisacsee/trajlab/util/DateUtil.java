package org.irisacsee.trajlab.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 日期工具
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class DateUtil {
    public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final ZoneId DEFAULT_ZONE_ID = ZoneId.of("UTC+8");
    public static final DateTimeFormatter DEFAULT_FORMATTER =
            DateTimeFormatter.ofPattern(DEFAULT_FORMAT).withZone(DEFAULT_ZONE_ID);

    public static ZonedDateTime parseDate(String timeFormat) {
        return ZonedDateTime.parse(timeFormat, DEFAULT_FORMATTER);
    }

    public static String format(ZonedDateTime time, String pattern) {
        return time == null ? "" : DateTimeFormatter.ofPattern(pattern).format(time);
    }

    public static long parseDateToTimeStamp(ZonedDateTime dateTime) {
        return dateTime.toEpochSecond();
    }

    public static ZonedDateTime timeToZonedTime(long time) {
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), DEFAULT_ZONE_ID);
    }
}
