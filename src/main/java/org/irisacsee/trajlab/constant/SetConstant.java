package org.irisacsee.trajlab.constant;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 数据集常量
 *
 * @author irisacsee
 * @since 2024/11/21
 */
public class SetConstant {
    public static final String START_TIME = "1970-01-01 00:00:00";
    public static final int SRID = 4326;
    public static final byte[] DATA_COUNT = Bytes.toBytes("data_count");
    public static final byte[] DATA_MBR = Bytes.toBytes("data_mbr");
    public static final byte[] DATA_START_TIME = Bytes.toBytes("data_start_time");
    public static final byte[] DATA_END_TIME = Bytes.toBytes("data_end_time");
}
