package org.irisacsee.trajlab.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.irisacsee.trajlab.conf.ILoadConfig;
import org.irisacsee.trajlab.constant.SparkConfKeyConstant;

/**
 * Spark工具
 *
 * @author irisacsee
 * @since 2024/07/22
 */
@Slf4j
public class SparkUtil {
    public static SparkSession createSession(ILoadConfig loadConfig, String className, boolean isLocal) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(SparkConfKeyConstant.FS_PERMISSIONS_UMASK_MODE, "022");
        if (loadConfig.getFsDefaultName() != null) {
            sparkConf.set(SparkConfKeyConstant.FS_DEFAULT_FS, loadConfig.getFsDefaultName());
        }
        sparkConf.set(SparkConfKeyConstant.SPARK_SERIALIZER, "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set(SparkConfKeyConstant.SPARK_KRYOSERIALIZER_BUFFER_MAX, "256m");
        sparkConf.set(SparkConfKeyConstant.SPARK_KRYOSERIALIZER_BUFFER, "64m");
        if (isLocal) {
            sparkConf.setMaster("local[*]");
        }
        switch (loadConfig.getInputType()) {
            case STANDALONE:
            case HDFS:
            case HBASE:
            case GEOMESA:
                return SparkSession.builder()
                        .appName(className + "_" + System.currentTimeMillis())
                        .config(sparkConf)
                        .getOrCreate();
            default:
                log.error("Only HDFS and HIVE are supported as the input resource!");
                throw new NoSuchMethodError();
        }
    }

    public static SparkSession createSession(String className, boolean isLocal) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(SparkConfKeyConstant.FS_PERMISSIONS_UMASK_MODE, "022");
        sparkConf.set(SparkConfKeyConstant.SPARK_SERIALIZER, "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set(SparkConfKeyConstant.SPARK_KRYOSERIALIZER_BUFFER_MAX, "256m");
        sparkConf.set(SparkConfKeyConstant.SPARK_KRYOSERIALIZER_BUFFER, "64m");
        if (isLocal) {
            sparkConf.setMaster("local[*]");
        }

        return SparkSession.builder()
                .appName(className + "_" + System.currentTimeMillis())
                .config(sparkConf)
                .getOrCreate();
    }

    public static SparkSession createSession(String className) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(SparkConfKeyConstant.FS_PERMISSIONS_UMASK_MODE, "022");
        sparkConf.set(SparkConfKeyConstant.SPARK_SERIALIZER, "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set(SparkConfKeyConstant.SPARK_KRYOSERIALIZER_BUFFER_MAX, "256m");
        sparkConf.set(SparkConfKeyConstant.SPARK_KRYOSERIALIZER_BUFFER, "64m");

        return SparkSession.builder()
                .appName(className + "_" + System.currentTimeMillis())
                .config(sparkConf)
                .getOrCreate();
    }
}
