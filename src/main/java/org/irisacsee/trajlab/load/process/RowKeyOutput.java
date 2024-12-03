package org.irisacsee.trajlab.load.process;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.irisacsee.trajlab.conf.Config;
import org.irisacsee.trajlab.load.Loader;
import org.irisacsee.trajlab.util.SparkUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class RowKeyOutput {
    public static void main(String[] args) throws JsonProcessingException {
//        String configPath = Objects
//                .requireNonNull(RowKeyOutput.class.getResource("/StoreConfig.json"))
//                .getPath();
//        String configString = IOUtils.readLocalTextFile(configPath);
//        Config config = Config.parse(configString);
//
//        IndexMeta coreIndexMeta = ((HBaseStoreConfig) config.getStoreConfig()).getDataSetMeta().getCoreIndexMeta();
//        IndexStrategy indexStrategy = coreIndexMeta.getIndexStrategy();
//        try (SparkSession sparkSession = SparkUtil.createSession(
//                config.getLoadConfig(), RowKeyOutput.class.getName(), true)) {
//            Loader loader = new Loader();
//            JavaRDD<Trajectory> trajRdd = loader.loadTrajectory(
//                    sparkSession,
//                    (StandaloneLoadConfig) config.getLoadConfig(),
//                    (TrajectoryConfig) config.getDataConfig());
//            List<String> list = trajRdd.map(trajectory -> {
//                byte[] bytes = indexStrategy.index(trajectory).getBytes();
//                StringBuilder sb = new StringBuilder("[");
//                sb.append(bytes[0]);
//                for (int i = 1; i < bytes.length; ++i) {
//                    sb.append(",").append(bytes[i]);
//                }
//                sb.append("]");
//                return sb.toString();
//            }).collect();
//            sparkSession.stop();
//
//            List<String> sortList = new ArrayList<>(list);
//            Collections.sort(sortList);
//            StringBuilder sb = new StringBuilder();
//            for (String s : sortList) {
//                sb.append(s).append('\n');
//            }
//            IOUtils.writeStringToFile("C:\\Users\\22772\\Desktop\\learn\\rowkey.txt", sb.toString());
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
    }
}
