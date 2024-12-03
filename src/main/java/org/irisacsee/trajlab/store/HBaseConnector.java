package org.irisacsee.trajlab.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.irisacsee.trajlab.constant.DBConstant;
import org.irisacsee.trajlab.index.IndexType;
import org.irisacsee.trajlab.meta.DataSetMeta;
import org.irisacsee.trajlab.meta.IndexMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * HBase连接器
 *
 * @author irisacsee
 * @since 2024/11/17
 */
public class HBaseConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConnector.class);
    private static final TableName META_TABLE_NAME = TableName.valueOf(DBConstant.META_TABLE_NAME);
    private static Configuration conf;
    private static Connection connection;

    /**
     * 获取Hadoop配置对象，目前为固定连接到超算集群
     *
     * @return Hadoop配置对象
     */
    public static Configuration getConf() {
        if (conf == null) {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "gisweb1"); // 设置ZooKeeper地址
            conf.set("hbase.zookeeper.property.clientPort", "2182"); // 设置ZooKeeper端口
        }
        return conf;
    }

    /**
     * 获取HBase连接对象
     *
     * @return HBase连接对象
     * @throws IOException 抛出IO异常
     */
    public static Connection getConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
            connection = ConnectionFactory.createConnection(getConf());
        }
        return connection;
    }

    /**
     * 关闭HBase连接
     *
     * @throws IOException 抛出IO异常
     */
    public static void closeConnection() throws IOException {
       if (connection != null && !connection.isClosed()) {
           connection.close();
       }
    }

    /**
     * 创建HBase表，并添加协处理器
     *
     * @param tableName    表名
     * @param columnFamily 列簇名
     * @param splits       预分区键
     * @throws IOException 抛出IO异常
     */
    public static void createTable(String tableName, String columnFamily, byte[][] splits) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
//                hTableDescriptor.addCoprocessor("org.irisacsee.trajlab.query.coprocessor.STQueryEndPoint");
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
                hTableDescriptor.addFamily(hColumnDescriptor);
                if (splits != null) {
                    admin.createTable(hTableDescriptor, splits);
                } else {
                    admin.createTable(hTableDescriptor);
                }
            }
        }
    }

    public static void createTables(DataSetMeta dataSetMeta) throws IOException {
        Map<IndexType, List<IndexMeta>> indexMetaMap = dataSetMeta.getAvailableIndexes();
        for (IndexType indexType : indexMetaMap.keySet()) {
            for (IndexMeta indexMeta : indexMetaMap.get(indexType)) {
                createTable(indexMeta.getIndexTableName(), DBConstant.INDEX_TABLE_CF, indexMeta.getSplits());
                LOGGER.info("Table {} created.", indexMeta.getIndexTableName());
            }
        }
    }

    public static void createDataSet(DataSetMeta dataSetMeta) throws IOException {
        getConnection();
        createTable(DBConstant.META_TABLE_NAME, DBConstant.META_TABLE_COLUMN_FAMILY, null);
        String dataSetName = dataSetMeta.getDataSetName();
        if (dataSetExists(dataSetName)) {
            LOGGER.warn("The dataset(name: {}) already exists.", dataSetName);
            DataSetMeta update = getDataSetMeta(dataSetName);
            update.getIndexMetaList().addAll(dataSetMeta.getIndexMetaList());
            getMetaTable().putDataSet(update);
            return;
        }
        getMetaTable().putDataSet(dataSetMeta);
        LOGGER.info("Dataset {} created.", dataSetName);
    }

    public static MetaTable getMetaTable() throws IOException {
        return new MetaTable(connection.getTable(META_TABLE_NAME));
    }

    public static void main(String[] args) throws IOException {
//        getConnection();
//        System.out.println(getDataSetMeta("T100"));
        // 补充元信息
//        String[] dataSetNames = new String[]{"T20", "T40", "T60", "T80"};
//        for (String dataSetName : dataSetNames) {
//            DataSetMeta dataSetMeta = getDataSetMeta(dataSetName);
//            dataSetMeta.getIndexMetaList()
//        }

        String[] dataSetNames = new String[]{"T100ST", "T100TS"};
        String[] modes = new String[]{"point", "trajectory"};
        String[] runs = new String[]{"run", "runPartitioned"};
        int[] ks = new int[]{10, 50, 100, 500, 1000};

    }

    public static boolean dataSetExists(String datasetName) throws IOException {
        MetaTable metaTable = null;
        try {
            metaTable = getMetaTable();
            return metaTable.dataSetExists(datasetName);
        } finally {
            if (metaTable != null) metaTable.close();
        }
    }

    public static DataSetMeta getDataSetMeta(String dataSetName) throws IOException {
        MetaTable metaTable = null;
        try {
            metaTable = getMetaTable();
            return metaTable.getDataSetMeta(dataSetName);
        } finally {
            if (metaTable != null) metaTable.close();
        }
    }

//    public static void main(String[] args) throws IOException {
//        Connection conn = getConnection();
//        Admin admin = conn.getAdmin();
//        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("ods-test"));
//        tableDescriptor.addFamily(new HColumnDescriptor("cf1"));
//        admin.createTable(tableDescriptor);
//        System.out.println(admin.tableExists(TableName.valueOf("ods-test")));
//        closeConnection();
//    }
}
