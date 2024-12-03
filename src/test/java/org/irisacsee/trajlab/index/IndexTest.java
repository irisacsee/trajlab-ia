package org.irisacsee.trajlab.index;

import org.irisacsee.trajlab.index.impl.TXZ2IndexStrategy;
import org.irisacsee.trajlab.index.impl.XZ2IndexStrategy;
import org.irisacsee.trajlab.index.impl.XZ2TIndexStrategy;
import org.irisacsee.trajlab.index.impl.XZTIndexStrategy;
import org.irisacsee.trajlab.index.type.TimeLine;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.model.TrajectoryPoint;
import org.irisacsee.trajlab.query.condition.SpatialQueryCondition;
import org.irisacsee.trajlab.query.condition.SpatialTemporalQueryCondition;
import org.irisacsee.trajlab.query.condition.TemporalQueryCondition;
import org.irisacsee.trajlab.constant.QueryConstant;
import org.irisacsee.trajlab.util.GeoUtil;
import org.irisacsee.trajlab.util.IOUtil;
import org.locationtech.jts.geom.Geometry;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IndexTest {
    // 轨迹分段时间，即每段轨迹的时间间隔不超过该间隔
    private static final long SEGMENT_INTERVAL = 60 * 60 * 1000;
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String DEFAULT_ZONE_ID = "UTC+8";
    private static final DateTimeFormatter DEFAULT_FORMATTER =
            DateTimeFormatter.ofPattern(DEFAULT_FORMAT).withZone(ZoneId.of(DEFAULT_ZONE_ID));
    private static final TemporalQueryCondition TQC;
    private static int tid = 0;

    static {
        ZonedDateTime start = ZonedDateTime.parse("2008-02-02 00:00:00", DEFAULT_FORMATTER);
        ZonedDateTime end = ZonedDateTime.parse("2008-02-03 00:00:00", DEFAULT_FORMATTER);
        TimeLine timeLine = new TimeLine(start, end);
        TQC = new TemporalQueryCondition(timeLine, TemporalQueryCondition.TemporalQueryType.INTERSECT);
    }

    public static void main(String[] args) {
        String content = IOUtil.readLocalTextFileLine("C:\\xc\\Tool\\BigData\\tdrive\\release\\test\\1.txt");
        List<Trajectory> trajectories = parse(content);
//        XZ2IndexStrategy xz2IndexStrategy = new XZ2IndexStrategy();
//        testIndexBuild(trajectories, xz2IndexStrategy);
//        XZTIndexStrategy xztIndexStrategy = new XZTIndexStrategy();
//        testIndexBuild(trajectories, xztIndexStrategy);
//        XZ2TIndexStrategy xz2tIndexStrategy = new XZ2TIndexStrategy();
//        testIndexBuild(trajectories, xz2tIndexStrategy);
        TXZ2IndexStrategy txz2IndexStrategy = new TXZ2IndexStrategy();
        testIndexBuild(trajectories, txz2IndexStrategy);
//        testScanRanges(trajectories, indexStrategy);
//        testPartitionScanRanges(trajectories, indexStrategy);

//        String path = "C:\\xc\\Tool\\BigData\\tdrive\\release\\10";
//        List<String> fileNames = IOUtil.getFileNames(path);
//        XZ2TIndexStrategy indexStrategy = new XZ2TIndexStrategy();
//        List<Trajectory> trajectories = new ArrayList<>();
//        for (String fileName : fileNames) {
//            String content = IOUtil.readLocalTextFileLine(fileName);
//            trajectories.addAll(parse(content));
//        }
//        for (Trajectory t : trajectories) {
//            System.out.println(Arrays.toString(indexStrategy.index(t)));
//        }
//        System.out.println(trajectories.size());
    }

    private static void testIndexBuild(List<Trajectory> trajectories, IndexStrategy indexStrategy) {
        long start = System.currentTimeMillis();
//        for (int i = 0; i < 10000; ++i) {
            for (Trajectory trajectory : trajectories) {
                indexStrategy.index(trajectory);
//                System.out.println(Arrays.toString(indexStrategy.index(trajectory)));
            }
//        }
        System.out.println("cost: " + (System.currentTimeMillis() - start) + "ms");
    }

    private static void testScanRanges(List<Trajectory> trajectories, XZ2TIndexStrategy indexStrategy) {
        long start = System.currentTimeMillis();
        for (Trajectory trajectory : trajectories) {
            Geometry buffer = trajectory.buffer(GeoUtil.getDegreeFromKm(QueryConstant.BASIC_BUFFER_DISTANCE));
            SpatialQueryCondition sqc = new SpatialQueryCondition(buffer, SpatialQueryCondition.SpatialQueryType.INTERSECT);
            SpatialTemporalQueryCondition queryCondition = new SpatialTemporalQueryCondition(sqc, TQC);
            System.out.println(indexStrategy.getScanRanges(queryCondition).size());
        }
        System.out.println("cost: " + (System.currentTimeMillis() - start) + "ms");
    }

    private static void testPartitionScanRanges(List<Trajectory> trajectories, XZ2TIndexStrategy indexStrategy) {
        long start = System.currentTimeMillis();
        for (Trajectory trajectory : trajectories) {
            Geometry buffer = trajectory.buffer(GeoUtil.getDegreeFromKm(QueryConstant.BASIC_BUFFER_DISTANCE));
            SpatialQueryCondition sqc = new SpatialQueryCondition(buffer, SpatialQueryCondition.SpatialQueryType.INTERSECT);
            SpatialTemporalQueryCondition queryCondition = new SpatialTemporalQueryCondition(sqc, TQC);
            System.out.println(indexStrategy.getPartitionedScanRanges(queryCondition).size());
        }
        System.out.println("cost: " + (System.currentTimeMillis() - start) + "ms");
    }

    private static List<Trajectory> parse(String content) {
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
                append(trajectories, oid, trajPoints);
                trajPoints = new ArrayList<>();
                trajPoints.add(trajPoint);
                prev = curr;
            }
        }
        if (!trajPoints.isEmpty() && trajPoints.size() > 1) {
            append(trajectories, oid, trajPoints);
        }
        return trajectories;
    }

    private static void append(
            List<Trajectory> trajectories,
            String oid,
            List<TrajectoryPoint> trajPoints) {
        Trajectory trajectory = new Trajectory(String.valueOf(tid), oid, trajPoints, true);
        trajectories.add(trajectory);
        ++tid;
    }
}
