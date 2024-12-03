package org.irisacsee.trajlab.store;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.irisacsee.trajlab.conf.HBaseStoreConfig;
import org.irisacsee.trajlab.constant.DBConstant;
import org.irisacsee.trajlab.index.IndexStrategy;
import org.irisacsee.trajlab.index.type.KeyFamilyQualifier;
import org.irisacsee.trajlab.meta.DataSetMeta;
import org.irisacsee.trajlab.meta.IndexMeta;
import org.irisacsee.trajlab.meta.SetMeta;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.model.TrajectoryMapper;
import org.irisacsee.trajlab.store.mapper.MainToMainMapper;
import org.irisacsee.trajlab.store.mapper.MainToSecondaryMapper;
import org.irisacsee.trajlab.util.SerializableFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * HBase存储器
 *
 * @author irisacsee
 * @since 2024/11/19
 */
@Slf4j
public class HBaseStore extends Configured implements Serializable {
    private static final Connection HBASE_CLIENT;
    private final HBaseStoreConfig storeConfig;
    private final Path tempPath;

    static {
        try {
            HBASE_CLIENT = HBaseConnector.getConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public HBaseStore(HBaseStoreConfig hBaseStoreConfig, Configuration conf) {
        storeConfig = hBaseStoreConfig;
        tempPath = new Path(hBaseStoreConfig.getLocation());
        this.setConf(conf);
    }

    /**
     * 存储轨迹数据
     *
     * @param trajectoryRdd 轨迹RDD
     * @throws Exception 抛出IO异常
     */
    public void storeTrajectory(JavaRDD<Trajectory> trajectoryRdd) throws Exception {
        DataSetMeta dataSetMeta = storeConfig.getDataSetMeta();
        log.info("Starting bulk load dataset {}", dataSetMeta.getDataSetName());
        long startLoadTime = System.currentTimeMillis();
        log.info("Start storing BasePointTrajectory into location : " + this.storeConfig.getLocation());
        trajectoryRdd.cache();
        SetMeta setMeta = new SetMeta(trajectoryRdd);
        dataSetMeta.setSetMeta(setMeta);
        HBaseConnector.createDataSet(dataSetMeta);
        HBaseConnector.createTables(dataSetMeta);
        IndexMeta coreIndexMeta = storeConfig.getDataSetMeta().getMainIndexMeta();
        log.info("Starting bulk load main index, meta: {}", coreIndexMeta);
        try {
            bulkLoadToMainIndex(trajectoryRdd, coreIndexMeta);
        } catch (Exception e) {
            log.error("Failed to finish bulk load main index {}", coreIndexMeta, e);
            throw e;
        }
        log.info("Successfully bulkLoad to main index, meta: {}", coreIndexMeta);
        try {
            bulkLoadToSecondaryIndex(dataSetMeta);
        } catch (Exception e) {
            log.error("Failed to finish bulk load second index {}", dataSetMeta.getIndexMetaList(), e);
            throw e;
        }
        log.info("Successfully bulkLoad to second index, meta: {}", dataSetMeta.getIndexMetaList());
        long endLoadTime = System.currentTimeMillis();
        log.info("DataSet {} store finished, cost time: {}ms.",
                dataSetMeta.getDataSetName(), (endLoadTime - startLoadTime));
        trajectoryRdd.unpersist();
        deleteTempHPath();
        HBaseConnector.closeConnection();
    }

    /**
     * BulkLoad批量导入主索引
     * @param trajectoryRdd 轨迹RDD
     * @param mainIndexMeta 主索引元数据
     * @throws Exception 抛出IO异常
     */
    public void bulkLoadToMainIndex(JavaRDD<Trajectory> trajectoryRdd, IndexMeta mainIndexMeta) throws Exception {
        String mainTableName = mainIndexMeta.getIndexTableName();
        Job job = Job.getInstance(getConf(), "Batch Import HBase Table：" + mainTableName);
        cancelDeleteOnExit(job);

        TableName tableName = TableName.valueOf(mainTableName);
        Table table = HBASE_CLIENT.getTable(tableName);
        RegionLocator locator = HBASE_CLIENT.getRegionLocator(tableName);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HFileOutputFormat2.configureIncrementalLoad(job, table, locator);

        IndexStrategy<?> indexStrategy = mainIndexMeta.getIndexStrategy();
        long start = System.currentTimeMillis();
        JavaRDD<Put> putJavaRDD = trajectoryRdd.map(trajectory ->
                TrajectoryMapper.mapTrajectoryToMainIndexPut(trajectory, indexStrategy));
        SerializableFunction<Put, List<Tuple2<KeyFamilyQualifier, KeyValue>>> mapPutToKeyValue =
                TrajectoryMapper.getMapPutToKeyValue(true);
        JavaPairRDD<KeyFamilyQualifier, KeyValue> putJavaKeyValueRdd = putJavaRDD
                .flatMapToPair(output -> mapPutToKeyValue.apply(output).iterator());
        JavaPairRDD<ImmutableBytesWritable, KeyValue> putJavaPairRdd = putJavaKeyValueRdd
                .sortByKey(true)
                .mapToPair(cell -> new Tuple2<>(new ImmutableBytesWritable(cell._1.getRowKey()), cell._2));

        putJavaPairRdd.saveAsNewAPIHadoopFile(
                storeConfig.getLocation(),
                ImmutableBytesWritable.class,
                KeyValue.class,
                HFileOutputFormat2.class);
        log.info("Build index cost {}ms", System.currentTimeMillis() - start);

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
        loader.doBulkLoad(tempPath, HBASE_CLIENT.getAdmin(), table, locator);
    }

    /**
     * BulkLoad批量导入辅助索引
     *
     * @param dataSetMeta 数据集元信息
     * @throws IOException 抛出IO异常
     */
    public void bulkLoadToSecondaryIndex(DataSetMeta dataSetMeta) throws IOException {
        Configuration conf = getConf();
        String location = storeConfig.getLocation();
        conf.set(DBConstant.BULK_LOAD_TEMP_FILE_PATH_KEY, location);
        for (IndexMeta im : dataSetMeta.getIndexMetaList()) {
            if (im != dataSetMeta.getMainIndexMeta()) {
                String indexTableName = im.getIndexTableName();
                conf.set(DBConstant.BULKLOAD_TARGET_INDEX_NAME, indexTableName);
                createIndexFromTable(conf, im, dataSetMeta);
            }
        }
    }

    /**
     * 本方法以核心索引表中的轨迹为数据源，按照新增IndexMeta转换，并BulkLoad至HBase中。
     * 执行此方法时，应确保DataSetMeta中已有本Secondary Table的信息。
     */
    public void createIndexFromTable(Configuration conf, IndexMeta indexMeta, DataSetMeta dataSetMeta) throws IOException {
        Path outPath = new Path(conf.get(DBConstant.BULK_LOAD_TEMP_FILE_PATH_KEY));
        String inputTableName = dataSetMeta.getMainIndexMeta().getIndexTableName();
        String outTableName = indexMeta.getIndexTableName();
        Job job = Job.getInstance(conf, "Batch Import HBase Table：" + outTableName);
        job.setJarByClass(HBaseStore.class);
        // 设置MapReduce任务输出的路径
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 配置Map算子，根据待写入Index是否为主索引，选择对应的Mapper实现。
        job.getConfiguration().set(DBConstant.BULKLOAD_TARGET_INDEX_NAME, outTableName);
        job.getConfiguration().setBoolean(DBConstant.ENABLE_SIMPLE_SECONDARY_INDEX, indexMeta.getIndexTableName().contains("simple"));

        if (indexMeta.isMainIndex()) {
            TableMapReduceUtil.initTableMapperJob(inputTableName,
                    buildCoreIndexScan(),
                    MainToMainMapper.class,
                    ImmutableBytesWritable.class,
                    Put.class,
                    job);
        } else {
            TableMapReduceUtil.initTableMapperJob(inputTableName,
                    buildCoreIndexScan(),
                    MainToSecondaryMapper.class,
                    ImmutableBytesWritable.class,
                    Put.class,
                    job);
        }

        // 配置Reduce算子
        job.setReducerClass(PutSortReducer.class);

        RegionLocator locator = HBaseConnector.getConnection().getRegionLocator(TableName.valueOf(outTableName));
        try (Admin admin = HBaseConnector.getConnection().getAdmin();
             Table table = HBaseConnector.getConnection().getTable(TableName.valueOf(outTableName))) {
            HFileOutputFormat2.configureIncrementalLoad(job, table, locator);
            cancelDeleteOnExit(job);
            if (!job.waitForCompletion(true)) {
                return;
            }
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(outPath, admin, table, locator);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Scan buildCoreIndexScan() {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(DBConstant.INDEX_TABLE_CF));
        return scan;
    }

    public void cancelDeleteOnExit(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        String partitionsFile = conf.get(TotalOrderPartitioner.PARTITIONER_PATH, TotalOrderPartitioner.DEFAULT_PATH);
        Path partitionsPath = new Path(partitionsFile);
        fs.makeQualified(partitionsPath);
        fs.cancelDeleteOnExit(partitionsPath);
    }

    /**
     * 删除临时存储
     *
     * @throws IOException 抛出IO异常
     */
    public void deleteTempHPath() throws IOException {
        FileSystem fs = tempPath.getFileSystem(getConf());
        if (fs.exists(tempPath)) {
            fs.delete(tempPath, true);
        }
        fs.close();
    }
}
