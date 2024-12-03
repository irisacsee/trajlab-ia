package org.irisacsee.trajlab.store.process;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.irisacsee.trajlab.conf.Config;
import org.irisacsee.trajlab.conf.HBaseStoreConfig;
import org.irisacsee.trajlab.conf.StandaloneLoadConfig;
import org.irisacsee.trajlab.index.type.TimeLine;
import org.irisacsee.trajlab.meta.DataSetMeta;
import org.irisacsee.trajlab.meta.IndexMeta;
import org.irisacsee.trajlab.meta.SetMeta;
import org.irisacsee.trajlab.model.MinimumBoundingBox;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.model.TrajectoryFeatures;
import org.irisacsee.trajlab.model.TrajectoryMapper;
import org.irisacsee.trajlab.model.TrajectoryPoint;
import org.irisacsee.trajlab.store.HBaseConnector;
import org.irisacsee.trajlab.util.IOUtil;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 数据存储过程
 *
 * @author irisacsee
 * @since 2024/07/22
 */
@Slf4j
public class StoreProcess {
    private static final long SEGMENT_INTERVAL = 60 * 60 * 1000;
    private static final String deaultFormat = "yyyy-MM-dd HH:mm:ss";
    private static final String defaultZoneId = "UTC+8";
    private static final DateTimeFormatter defaultFormatter =
            DateTimeFormatter.ofPattern(deaultFormat).withZone(ZoneId.of(defaultZoneId));
    private static int tid = 1;
    private static Config config;
    private static MinimumBoundingBox box = null;
    private static ZonedDateTime startTime = null;
    private static ZonedDateTime endTime = null;
    private static int count = 0;
    private static IndexMeta mainIndexMeta;

    public static void main(String[] args) throws JsonProcessingException, IOException {
//        storeBySpark();
        storeByHBasePut();
    }

    private static void storeByHBasePut() throws IOException {
        long start = System.currentTimeMillis();
        String configPath = Objects
                .requireNonNull(StoreProcess.class.getResource("/StoreConfig.json"))
                .getPath();
        String configString = IOUtil.readLocalTextFile(configPath);
        config = Config.parse(configString);
        long end = System.currentTimeMillis();
        log.info("Read config file cost {} ms", end - start);
        System.out.println(((HBaseStoreConfig) config.getStoreConfig()).getMainIndex());

        start = System.currentTimeMillis();
        Connection instance = HBaseConnector.getConnection();
        HBaseStoreConfig storeConfig = (HBaseStoreConfig) config.getStoreConfig();
        DataSetMeta dataSetMeta = storeConfig.getDataSetMeta();
        HBaseConnector.createTables(dataSetMeta);
        end = System.currentTimeMillis();
        log.info("Create index tables cost {} ms", end - start);

        start = System.currentTimeMillis();
        StandaloneLoadConfig loadConfig = (StandaloneLoadConfig) config.getLoadConfig();
        List<String> fileNames = IOUtil.getFileNames(loadConfig.getLocation());
        end = System.currentTimeMillis();
        log.info("Read filenames cost {} ms", end - start);

        // 解析并存储数据
        start = System.currentTimeMillis();
        mainIndexMeta = dataSetMeta.getMainIndexMeta();
        String mainTableName = mainIndexMeta.getIndexTableName();
        Table table = instance.getTable(TableName.valueOf(mainTableName));
//        List<Trajectory> trajectories = new ArrayList<>(947118);
//        StringBuilder builder = new StringBuilder();
        for (String fileName : fileNames) {
            csvParse(fileName, table);
        }
        end = System.currentTimeMillis();
        log.info("Parse trajectories cost {} ms, count: {}", end - start, count);

        // 创建元数据表
        start = System.currentTimeMillis();
        SetMeta setMeta = new SetMeta(box, new TimeLine(startTime, endTime), count);
        dataSetMeta.setSetMeta(setMeta);
        HBaseConnector.createDataSet(dataSetMeta);
        end = System.currentTimeMillis();
        log.info("Create table cost {} ms", end - start);

        // 存储数据
//        start = System.currentTimeMillis();
//        IndexMeta coreIndexMeta = dataSetMeta.getCoreIndexMeta();
//        String mainTableName = coreIndexMeta.getIndexTableName();
//        Table table = instance.getTable(mainTableName);
//        for (Trajectory trajectory : trajectories) {
//            Put put = TrajectoryDataMapper.mapTrajectoryToSingleRow(trajectory, coreIndexMeta);
//            table.put(put);
//        }
//        end = System.currentTimeMillis();
//        log.info("Store data cost {} ms", end - start);

        HBaseConnector.closeConnection();
        log.info("Finish");
    }

    private static void csvParse(String fileName,
//                                 List<Trajectory> trajectories,
                                 Table table) throws IOException {
        String[] fileNameSplit = fileName.split("\\\\");
        String name = fileNameSplit[fileNameSplit.length - 1].split("\\.")[0];
        List<String> lines = IOUtil.readCSV(fileName, false);
        if (!lines.isEmpty()) {
            List<TrajectoryPoint> trajPoints = new ArrayList<>();
            String[] firsts = lines.get(0).split(",");
            String oid = firsts[0];
            long prev = Timestamp.valueOf(firsts[1]).getTime();
//            System.out.println("timeStr: " + timeStr + ", prev: " + prev);
//            int len = 0;
            for (String line : lines) {
                String[] strs = line.split(",");
                String timeStr = strs[1];
                long curr = Timestamp.valueOf(timeStr).getTime();
                ZonedDateTime time = ZonedDateTime.parse(timeStr, defaultFormatter);
                double lng = Double.parseDouble(strs[2]);
                double lat = Double.parseDouble(strs[3]);
                TrajectoryPoint trajPoint = new TrajectoryPoint(time, lng, lat);
                if (curr - prev < SEGMENT_INTERVAL) {
//                    append(trajPoints, line);
                    trajPoints.add(trajPoint);
//                    ++len;
                } else if (trajPoints.size() == 1) {
//                    append(trajPoints, line);
                    trajPoints = new ArrayList<>();  // 直接丢弃这个点
                    trajPoints.add(trajPoint);
                    prev = curr;
//                    ++len;
//                        System.out.println("this tid: " + tid);
                } else {
                    output(oid, trajPoints, table);
//                    append(trajPoints, line);
                    trajPoints = new ArrayList<>();
                    trajPoints.add(trajPoint);
                    prev = curr;
//                    len = 1;
                }
            }
            if (!trajPoints.isEmpty() && trajPoints.size() > 1) {
                output(oid, trajPoints, table);
            }
        } else {
            System.out.println("empty file: " + name);
        }
    }

//    private static void append(StringBuilder builder, String line) {
//        builder.append(tid);
//        builder.append(",");
//        builder.append(line);
//        builder.append("\r\n");
//    }

    private static void output(String oid,
                               List<TrajectoryPoint> trajPoints,
//                               List<Trajectory> trajectories,
                               Table table) throws IOException {
//        IOUtils.writeStringToFile(OUTPUT_PATH + "\\" + name + "_" + tid + ".txt", builder.toString());
//        Trajectory trajectory = CSV2Traj.multifileParse(builder.toString(), (TrajectoryConfig) config.getDataConfig(), ",");
        Trajectory trajectory = new Trajectory(String.valueOf(tid), oid, trajPoints, true);
        TrajectoryFeatures features = trajectory.getTrajectoryFeatures();
        if (box == null) {
            box = features.getMbr();
            startTime = features.getStartTime();
            endTime = features.getEndTime();
        } else {
            box = box.union(features.getMbr());
            startTime =
                    startTime.compareTo(features.getStartTime()) <= 0
                            ? startTime
                            : features.getStartTime();
            endTime =
                    endTime.compareTo(features.getEndTime()) >= 0
                            ? endTime
                            : features.getEndTime();
        }
        ++count;
//        trajectories.add(trajectory);
        ++tid;
        Put put = TrajectoryMapper.mapTrajectoryToMainIndexPut(trajectory, mainIndexMeta.getIndexStrategy());
        table.put(put);
//        builder.delete(0, builder.length());
    }


//    private static void storeBySpark() throws JsonProcessingException {
//        String configPath = Objects
//                .requireNonNull(StoreProcess.class.getResource("/StoreConfig.json"))
//                .getPath();
//        String configString = IOUtils.readLocalTextFile(configPath);
//        Config config = Config.parse(configString);
//        log.info("Init spark session");
//        boolean isLocal = true;
//        try (SparkSession sparkSession = SparkUtil.createSession(
//                config.getLoadConfig(), StoreProcess.class.getName(), isLocal)) {
//            ILoader loader = ILoader.getLoader(config.getLoadConfig());
//            JavaRDD<Trajectory> trajRdd = loader.loadTrajectory(
//                    sparkSession, config.getLoadConfig(), config.getDataConfig());
////            trajRdd.take(10).forEach(trajectory -> System.out.println(trajectory.getTrajectoryID()));
//            JavaRDD<Trajectory> featuresRdd = trajRdd.map(trajectory -> {
//                trajectory.getTrajectoryFeatures();
//                return trajectory;
//            });
//            IStore store = IStore.getStore(config.getStoreConfig());
//            store.storeTrajectory(featuresRdd);
//            log.info("Store process finished");
//            sparkSession.stop();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
}
