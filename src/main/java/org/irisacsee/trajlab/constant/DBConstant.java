package org.irisacsee.trajlab.constant;

import org.apache.hadoop.hbase.util.Bytes;
import org.irisacsee.trajlab.model.Trajectory;

/**
 * 数据库常量
 *
 * @author irisacsee
 * @since 2024/11/21
 */
public class DBConstant {
    // Tables
    public static final String META_TABLE_NAME = "TrajLab_db_meta";
    public static final String META_TABLE_COLUMN_FAMILY = "meta";
    public static final String META_TABLE_INDEX_META_QUALIFIER = "index_meta";
    public static final String META_TABLE_CORE_INDEX_META_QUALIFIER = "main_table_index_meta";
    public static final String META_TABLE_DESC_QUALIFIER = "desc";

    // INDEX TABLE COLUMNS
    public static final String DATA_TABLE_SUFFIX = "_data";
    public static String INDEX_TABLE_CF = "cf0";
    public static final byte[] COLUMN_FAMILY = Bytes.toBytes(INDEX_TABLE_CF);
    public static final byte[] TRAJECTORY_ID_QUALIFIER = Bytes.toBytes(Trajectory.Schema.TRAJECTORY_ID);
    public static final byte[] OBJECT_ID_QUALIFIER = Bytes.toBytes(Trajectory.Schema.OBJECT_ID);
    public static final byte[] MBR_QUALIFIER = Bytes.toBytes(Trajectory.Schema.MBR);
    public static final byte[] START_POINT_QUALIFIER = Bytes.toBytes(Trajectory.Schema.START_POSITION);
    public static final byte[] END_POINT_QUALIFIER = Bytes.toBytes(Trajectory.Schema.END_POSITION);
    public static final byte[] START_TIME_QUALIFIER = Bytes.toBytes(Trajectory.Schema.START_TIME);
    public static final byte[] END_TIME_QUALIFIER = Bytes.toBytes(Trajectory.Schema.END_TIME);
    public static final byte[] TRAJ_POINTS_QUALIFIER = Bytes.toBytes(Trajectory.Schema.TRAJ_POINTS);
    public static final byte[] PTR_QUALIFIER = Bytes.toBytes(Trajectory.Schema.PTR);
    public static final byte[] POINT_NUMBER_QUALIFIER = Bytes.toBytes(Trajectory.Schema.POINT_NUMBER);
    public static final byte[] SPEED_QUALIFIER = Bytes.toBytes(Trajectory.Schema.SPEED);
    public static final byte[] LENGTH_QUALIFIER = Bytes.toBytes(Trajectory.Schema.LENGTH);
    public static final byte[] EXT_VALUES_QUALIFIER = Bytes.toBytes(Trajectory.Schema.EXT_VALUES);

    // Bulk load
    public static final String BULK_LOAD_TEMP_FILE_PATH_KEY = "import.file.output.path";
    public static final String BULK_LOAD_INPUT_FILE_PATH_KEY = "import.process.input.path";
    public static final String BULKLOAD_TARGET_INDEX_NAME = "bulkload.target.index.name";
    public static final String BULKLOAD_TEXT_PARSER_CLASS = "bulkload.parser.class";
    public static final String ENABLE_SIMPLE_SECONDARY_INDEX = "enable.simple.secondary.index";

    // Connection
    public static final String OPEN_CONNECTION_FAILED = "Cannot connect to data base.";
    public static final String CLOSE_CONNECTION_FAILED = "Close connection failed.";

    // Initial
    public static final String INITIAL_FAILED = "Initial failed.";
}
