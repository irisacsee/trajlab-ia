package org.irisacsee.trajlab.constant;

import org.irisacsee.trajlab.index.type.TimePeriod;

import java.time.ZoneId;

/**
 * 编码常量
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class CodeConstant {
    /**
     * Max length of xz2 Quadrant sequence
     */
    public static final short MAX_XZ2_PRECISION = 16;

    public static final double XZ2_X_MIN = -180.0;
    public static final double XZ2_X_MAX = 180.0;
    public static final double XZ2_Y_MIN = -90.0;
    public static final double XZ2_Y_MAX = 90.0;

    public static final short MAX_TIME_BIN_PRECISION = 7;
    public static final ZoneId TIME_ZONE = ZoneId.of("UTC+8");

    public static final TimePeriod DEFAULT_TIME_PERIOD = TimePeriod.DAY;

    public static final double LOG_FIVE = Math.log(0.5);
    public static final int MAX_OID_LENGTH = 20;
    public static final int MAX_TID_LENGTH = 20;
}
