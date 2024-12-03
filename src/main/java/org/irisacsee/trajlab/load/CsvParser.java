package org.irisacsee.trajlab.load;

import org.apache.spark.util.LongAccumulator;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.model.TrajectoryPoint;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class CsvParser {
    // 轨迹分段时间，即每段轨迹的时间间隔不超过该间隔
    private static final long SEGMENT_INTERVAL = 60 * 60 * 1000;
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String DEFAULT_ZONE_ID = "UTC+8";
    private static final DateTimeFormatter DEFAULT_FORMATTER =
            DateTimeFormatter.ofPattern(DEFAULT_FORMAT).withZone(ZoneId.of(DEFAULT_ZONE_ID));

    public static List<Trajectory> parse(String content, LongAccumulator tid) {
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
