package org.irisacsee.trajlab.store.process;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.irisacsee.trajlab.conf.Config;
import org.irisacsee.trajlab.conf.HBaseStoreConfig;
import org.irisacsee.trajlab.conf.StandaloneLoadConfig;
import org.irisacsee.trajlab.conf.TrajectoryConfig;
import org.irisacsee.trajlab.load.Loader;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.store.HBaseConnector;
import org.irisacsee.trajlab.store.HBaseStore;
import org.irisacsee.trajlab.util.IOUtil;
import org.irisacsee.trajlab.util.SparkUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 轨迹数据批量存储
 *
 * @author irisacsee
 * @since 2024/11/19
 */
public class StoreBatch {
    private static final Logger LOGGER = LoggerFactory.getLogger(StoreBatch.class);

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
//        String configPath = Objects
//                .requireNonNull(StoreBatch.class.getResource(args[0]))
//                .getPath();
        String configString = IOUtil.readLocalTextFile(args[0]);
        Config config = Config.parse(configString);
        LOGGER.info("Read config file cost {}ms", System.currentTimeMillis() - start);
        String appName = args[0].split("\\.")[0];

        try (SparkSession sparkSession = SparkUtil.createSession(
                config.getLoadConfig(), appName, false)) {
            Loader loader = new Loader();
            start = System.currentTimeMillis();
            JavaRDD<Trajectory> trajRdd = loader.loadTrajectory(
                    sparkSession, (StandaloneLoadConfig) config.getLoadConfig());
            JavaRDD<Trajectory> featuresRdd = trajRdd.map(trajectory -> {
                trajectory.getTrajectoryFeatures();
                return trajectory;
            });
            HBaseStore store = new HBaseStore((HBaseStoreConfig) config.getStoreConfig(), HBaseConnector.getConf());
            store.storeTrajectory(featuresRdd);
            LOGGER.info("Store batch process cost {}ms", System.currentTimeMillis() - start);
            LOGGER.info("Store batch process finished");
            sparkSession.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
