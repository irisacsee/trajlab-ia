package org.irisacsee.trajlab.load;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.irisacsee.trajlab.conf.StandaloneLoadConfig;
import org.irisacsee.trajlab.conf.enums.FileType;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.model.TrajectoryPoint;
import scala.Tuple2;

import javax.ws.rs.NotSupportedException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 加载器
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class Loader implements Serializable {
    // 轨迹分段时间，即每段轨迹的时间间隔不超过该间隔
    private static final long SEGMENT_INTERVAL = 60 * 60 * 1000;
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String DEFAULT_ZONE_ID = "UTC+8";
    private static final DateTimeFormatter DEFAULT_FORMATTER =
            DateTimeFormatter.ofPattern(DEFAULT_FORMAT).withZone(ZoneId.of(DEFAULT_ZONE_ID));

    public Loader() {}

    /**
     * 加载轨迹数据
     *
     * @param ss                   SparkSession
     * @param standaloneLoadConfig 加载配置
     * @return 轨迹数据RDD
     */
    public JavaRDD<Trajectory> loadTrajectory(
            SparkSession ss,
            StandaloneLoadConfig standaloneLoadConfig) {
        int partNum = standaloneLoadConfig.getPartNum();
        JavaRDD<Tuple2<String, String>> loadRdd = ss
                .sparkContext()
                .wholeTextFiles(standaloneLoadConfig.getLocation(), partNum)
                .toJavaRDD()
                .filter(s -> !s._2.isEmpty());

        FileType fileType = standaloneLoadConfig.getFileType();
        if (Objects.requireNonNull(fileType) == FileType.csv) {
            LongAccumulator tid = ss.sparkContext().longAccumulator("tid");
            return loadRdd.flatMap(s -> csvParse(s._2, tid).iterator());
        }
        throw new NotSupportedException(
                "can't support fileType " + standaloneLoadConfig.getFileType().getFileTypeEnum());
    }

    private List<Trajectory> csvParse(String content, LongAccumulator tid) {
        String[] lines = content.split("\n");
        List<Trajectory> trajectories = new ArrayList<>();
        List<TrajectoryPoint> trajPoints = new ArrayList<>();
        String[] firsts = lines[0].split(",");
        String oid = firsts[0];
        long prev = Timestamp.valueOf(firsts[1]).getTime();
        for (String line : lines) {
            String[] strs = line.split(",");
            String timeStr = strs[1];
            long curr = Timestamp.valueOf(timeStr).getTime();
            ZonedDateTime time = ZonedDateTime.parse(timeStr, DEFAULT_FORMATTER);
            double lng = Double.parseDouble(strs[2]);
            double lat = Double.parseDouble(strs[3]);
            TrajectoryPoint trajPoint = new TrajectoryPoint(time, lng, lat);
            if (curr - prev < SEGMENT_INTERVAL) {
                trajPoints.add(trajPoint);
            } else if (trajPoints.size() == 1) {
                trajPoints = new ArrayList<>();  // 直接丢弃这个点
                trajPoints.add(trajPoint);
                prev = curr;
            } else {
                append(trajectories, tid, oid, trajPoints);
                trajPoints = new ArrayList<>();
                trajPoints.add(trajPoint);
                prev = curr;
            }
        }
        if (!trajPoints.isEmpty() && trajPoints.size() > 1) {
            append(trajectories, tid, oid, trajPoints);
        }
        return trajectories;
    }

    private static void append(
            List<Trajectory> trajectories,
            LongAccumulator tid,
            String oid,
            List<TrajectoryPoint> trajPoints) {
        Trajectory trajectory = new Trajectory(String.valueOf(tid.value()), oid, trajPoints, true);
        trajectories.add(trajectory);
        tid.add(1);
    }
}
