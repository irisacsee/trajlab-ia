package org.irisacsee.trajlab.util;

import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.model.TrajectoryMapper;
import org.irisacsee.trajlab.model.TrajectoryPoint;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class TrajFileToGeoJson {
    private static final String INPUT_PATH = "C:\\xc\\Tool\\BigData\\tdrive\\release\\taxi_log_2008_by_id";
    private static final String OUTPUT_PATH = "C:\\Users\\22772\\Desktop\\knn\\geojson1";
    private static final long SEGMENT_INTERVAL = 60 * 60 * 1000;
    private static final List<Trajectory> TRAJECTORIES = new ArrayList<>(100000);
    private static int tid = 1;
    private static int name = 1;
    private static boolean flag = true;

    public static void main(String[] args) throws IOException {
        process();
//        clear();
    }

    private static void process() throws IOException {
        long start = System.currentTimeMillis();
        List<String> fileNames = IOUtil.getFileNames(INPUT_PATH);
        for (int i = 0; i < fileNames.size() && flag; ++i) {
//            System.out.println("processing: " + fileName);
            String fileName = fileNames.get(i);
            List<String> lines = IOUtil.readCSV(fileName, false);
            if (!lines.isEmpty()) {
                List<TrajectoryPoint> trajPoints = new ArrayList<>();
                String[] firsts = lines.get(0).split(",");
                String oid = firsts[0];
                long prev = Timestamp.valueOf(firsts[1]).getTime();
                for (String line : lines) {
                    String[] strs = line.split(",");
                    String timeStr = strs[1];
                    long curr = Timestamp.valueOf(timeStr).getTime();
                    ZonedDateTime time = ZonedDateTime.parse(timeStr, DateUtil.DEFAULT_FORMATTER);
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
                        append(oid, trajPoints);
                        trajPoints = new ArrayList<>();
                        trajPoints.add(trajPoint);
                        prev = curr;
                    }
                }
                if (!trajPoints.isEmpty() && trajPoints.size() > 1) {
                    append(oid, trajPoints);
                }
            } else {
                System.out.println("empty file: " + fileName);
            }
        }
        if (!TRAJECTORIES.isEmpty()) {
            IOUtil.writeStringToFile(OUTPUT_PATH + "\\" + name + ".geojson",
                    TrajectoryMapper.mapTrajectoryListToGeoJson(TRAJECTORIES).toString());
            TRAJECTORIES.clear();
        }
        System.out.println("cost: " + (System.currentTimeMillis() - start));
        System.out.println("tid: " + tid);
    }

    private static void append(String oid, List<TrajectoryPoint> trajPoints) throws IOException {
        if (trajPoints.size() < 50) {
            Trajectory trajectory = new Trajectory(String.valueOf(tid), oid, trajPoints, true);
            TRAJECTORIES.add(trajectory);
//        System.out.println("size: " + TRAJECTORIES.size());
            if (TRAJECTORIES.size() == 100) {
//            System.out.println("output");
                IOUtil.writeStringToFile(OUTPUT_PATH + "\\" + name + ".geojson",
                        TrajectoryMapper.mapTrajectoryListToGeoJson(TRAJECTORIES).toString());
                TRAJECTORIES.clear();
                if (name == 5) {
                    flag = false;
                }
                ++name;
            }
        }
        ++tid;
    }

    private static void output(StringBuilder builder, String name) throws IOException {
        IOUtil.writeStringToFile(OUTPUT_PATH + "\\" + name + "_" + tid + ".txt", builder.toString());
        ++tid;
        builder.delete(0, builder.length());
    }

    private static void clear() throws IOException {
        List<String> fileNames = IOUtil.getFileNames(OUTPUT_PATH);
        for (String fileName : fileNames) {
            Files.delete(Paths.get(fileName));
        }
    }
}
