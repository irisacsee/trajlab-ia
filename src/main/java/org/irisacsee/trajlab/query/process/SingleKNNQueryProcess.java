package org.irisacsee.trajlab.query.process;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.sql.SparkSession;
import org.irisacsee.trajlab.constant.CodeConstant;
import org.irisacsee.trajlab.index.type.TimeLine;
import org.irisacsee.trajlab.meta.DataSetMeta;
import org.irisacsee.trajlab.model.BasePoint;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.model.TrajectoryMapper;
import org.irisacsee.trajlab.query.KNNQuery;
import org.irisacsee.trajlab.query.WKNNQuery;
import org.irisacsee.trajlab.query.condition.KNNQueryCondition;
import org.irisacsee.trajlab.query.condition.TemporalQueryCondition;
import org.irisacsee.trajlab.store.HBaseConnector;
import org.irisacsee.trajlab.util.IOUtil;
import org.irisacsee.trajlab.util.SparkUtil;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SingleKNNQueryProcess {
    private static final byte[] ROW_KEY = new byte[]{0, 13, 0, 0, 0, 1, 36, -79, 65, 92, 0, 0, 0, 0, 0, 54, 32, -4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 49, 48};
    private static final DataSetMeta XZ2T;
    private static final DataSetMeta TXZ2;

    private static final BasePoint CEN_POINT = new BasePoint(116.492, 39.914);
    private static final Trajectory CEN_TRAJECTORY = getCenTrajectory(ROW_KEY);

    private static final String[] DATA_SET_NAMES = new String[]{"T100ST", "T100TS"};
    private static final String[] MODES = new String[]{"point", "trajectory"};
    private static final String[] RUNS = new String[]{"run", "runPartitioned"};
    private static final int[] KS = new int[]{10, 50, 100, 500, 1000};
    private static final TimeLine[] TIME_LINES = new TimeLine[5];

//    private static final String[] DATA_SET_NAMES = new String[]{"T100ST"};
//    private static final String[] MODES = new String[]{"point"};
//    private static final String[] RUNS = new String[]{"runPartitioned"};
//    private static final int[] KS = new int[]{10, 50, 100};
//    private static final SparkSession SS = SparkUtil.createSession(SingleKNNQueryProcess.class.getName());

    static {
        DateTimeFormatter dateTimeFormatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(CodeConstant.TIME_ZONE);
        ZonedDateTime start1 = ZonedDateTime.parse("2008-02-02 17:00:00", dateTimeFormatter);
        ZonedDateTime end1 = ZonedDateTime.parse("2008-02-02 18:00:00", dateTimeFormatter);
        TimeLine testTimeLine1 = new TimeLine(start1, end1);
        TIME_LINES[0] = testTimeLine1;
        ZonedDateTime start2 = ZonedDateTime.parse("2008-02-02 16:00:00", dateTimeFormatter);
        ZonedDateTime end2 = ZonedDateTime.parse("2008-02-02 19:00:00", dateTimeFormatter);
        TimeLine testTimeLine2 = new TimeLine(start2, end2);
        TIME_LINES[1] = testTimeLine2;
        ZonedDateTime start3 = ZonedDateTime.parse("2008-02-02 15:00:00", dateTimeFormatter);
        ZonedDateTime end3 = ZonedDateTime.parse("2008-02-02 21:00:00", dateTimeFormatter);
        TimeLine testTimeLine3 = new TimeLine(start3, end3);
        TIME_LINES[2] = testTimeLine3;
        ZonedDateTime start4 = ZonedDateTime.parse("2008-02-02 12:00:00", dateTimeFormatter);
        ZonedDateTime end4 = ZonedDateTime.parse("2008-02-03 00:00:00", dateTimeFormatter);
        TimeLine testTimeLine4 = new TimeLine(start4, end4);
        TIME_LINES[3] = testTimeLine4;
        ZonedDateTime start5 = ZonedDateTime.parse("2008-02-02 00:00:00", dateTimeFormatter);
        ZonedDateTime end5 = ZonedDateTime.parse("2008-02-03 00:00:00", dateTimeFormatter);
        TimeLine testTimeLine5 = new TimeLine(start5, end5);
        TIME_LINES[4] = testTimeLine5;
//        TIME_LINES.add(testTimeLine2);
        try {
            HBaseConnector.getConnection();
            XZ2T = HBaseConnector.getDataSetMeta("T100ST");
            TXZ2 = HBaseConnector.getDataSetMeta("T100TS");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
//        getCenTrajectory(rowKey);
//        System.out.println(getCenTrajectory(rowKey));
        testKnnQuery(1000, 0, "T100ST");
//        testKnnQueryBySpark(10);
//        for (int k : KS2) {
//            testKNNQuery(k);
//        }
//        for (String dataSetName : DATA_SET_NAMES) {
//            for (String mode : MODES) {
//                for (String run : RUNS) {
//                    for (int k : KS) {
//                        testKnn(dataSetName, mode, run, k);
//                        System.gc();
//                        System.out.println("sleep 5s");
//                        Thread.sleep(5000);
//                    }
//                }
//            }
//        }
//        SS.stop();
    }

//    private static void testKnn(String dataSetName, String mode, String run, int k) throws IOException {
//        long start = System.currentTimeMillis();
//        TemporalQueryCondition tqc = new TemporalQueryCondition(TIME_LINES,
//                TemporalQueryCondition.TemporalQueryType.INTERSECT);
////        KNNQueryCondition knnQueryCondition = new KNNQueryCondition(k, CEN_TRAJECTORY, tqc);
//        DataSetMeta dataSetMeta = null;
//        if (dataSetName.equals("T100ST")) {
//            dataSetMeta = XZ2T;
//        } else if (dataSetName.equals("T100TS")) {
//            dataSetMeta = TXZ2;
//        }
//
//        System.out.println("dsn: " + dataSetName + ", mode: " + mode + ", run: " + run + ", k: " + k);
//        List<Trajectory> results = null;
//        if (mode.equals("point")) {
//            KNNQueryCondition knnQueryCondition = new KNNQueryCondition(k, CEN_POINT, tqc);
//            KNNQuery knnQuery = new KNNQuery(dataSetMeta, knnQueryCondition);
//            if (run.equals("run")) {
//                results = knnQuery.knnRunBySparkCasePoint(SS);
//            } else if (run.equals("runPartitioned")) {
//                results = knnQuery.knnRunPartitionedBySparkCasePoint(SS);
//            }
//        } else if (mode.equals("trajectory")) {
//            KNNQueryCondition knnQueryCondition = new KNNQueryCondition(k, CEN_TRAJECTORY, tqc);
//            KNNQuery knnQuery = new KNNQuery(
//                    HBaseConnector.getDataSetMeta(dataSetName), knnQueryCondition);
//            if (run.equals("run")) {
//                results = knnQuery.knnRunBySparkCaseTrajectory(SS);
//            } else if (run.equals("runPartitioned")) {
//                results = knnQuery.knnRunPartitionedBySparkCaseTrajectory(SS);
//            }
//        }
////            List<Trajectory> results = knnQuery.runBySpark(sparkSession);
//        if (results != null) {
//            System.out.println("size: " + results.size());
//        } else {
//            System.out.println("error!");
//        }
//        long end = System.currentTimeMillis();
//        System.out.println("cost: " + (end - start) + "ms");
//    }

    private static Trajectory getCenTrajectory(byte[] rowKey) {
        try {
            Connection connection = HBaseConnector.getConnection();
            Table table = connection.getTable(TableName.valueOf("T10-XZ2T-default"));
            Get get = new Get(rowKey);
            Trajectory result = TrajectoryMapper.getAllTrajectoryFromResult(table.get(get));
//            JSONObject jsonObject = TrajectoryMapper.mapTrajectoryToGeoJson(result);
//            IOUtil.writeStringToFile("/home/oge/ods/data/1.json", jsonObject.toString());
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void testKnnQuery(int k, int tIndex, String dataSetName) throws IOException {
        long start = System.currentTimeMillis();
//        Database instance = Database.getInstance();
        TemporalQueryCondition tqc = new TemporalQueryCondition(TIME_LINES[tIndex],
                TemporalQueryCondition.TemporalQueryType.INTERSECT);
//        KNNQueryCondition knnQueryCondition = new KNNQueryCondition(k, CEN_TRAJECTORY, tqc);
        KNNQueryCondition knnQueryCondition = new KNNQueryCondition(k, new BasePoint(116.492, 39.914), tqc);
        KNNQuery knnQuery = new KNNQuery(
                HBaseConnector.getDataSetMeta(dataSetName), knnQueryCondition);

//        List<Trajectory> results = knnQuery.runPartitioned();
        List<Trajectory> results = knnQuery.runPartitionedPq();
        System.out.println("size: " + results.size());
//            for (Trajectory result : results) {
//                System.out.println(result);
//            }
        long end = System.currentTimeMillis();
        System.out.println("cost: " + (end - start) + "ms");
        Set<String> set = new HashSet<>();
        results.forEach(r -> set.add(r.getOid()));
        System.out.println("set size: " + set.size());
//        JSONObject jsonObject = TrajectoryMapper.mapTrajectoryListToGeoJson(results);
//        IOUtil.writeStringToFile(
//                "C:\\Users\\22772\\Desktop\\模拟面试\\ts.json", jsonObject.toString());
    }

    private static void testWKnnQuery(int k) throws IOException {
        long start = System.currentTimeMillis();
//        Database instance = Database.getInstance();
        TemporalQueryCondition tqc = new TemporalQueryCondition(TIME_LINES[0],
                TemporalQueryCondition.TemporalQueryType.INTERSECT);
        KNNQueryCondition knnQueryCondition = new KNNQueryCondition(k, CEN_TRAJECTORY, tqc);
        WKNNQuery knnQuery = new WKNNQuery(
                HBaseConnector.getDataSetMeta("T10"), knnQueryCondition);

        List<Trajectory> results = knnQuery.runPartitioned();
        System.out.println("size: " + results.size());
//            for (Trajectory result : results) {
//                System.out.println(result);
//            }
        long end = System.currentTimeMillis();
        System.out.println("cost: " + (end - start) + "ms");
    }

    private static void testKnnQueryBySpark(int k) throws IOException {
        long start = System.currentTimeMillis();
//        Database instance = Database.getInstance();
        TemporalQueryCondition tqc = new TemporalQueryCondition(TIME_LINES[0],
                TemporalQueryCondition.TemporalQueryType.INTERSECT);
//        KNNQueryCondition knnQueryCondition = new KNNQueryCondition(k, CEN_TRAJECTORY, tqc);
        KNNQueryCondition knnQueryCondition = new KNNQueryCondition(k, new BasePoint(116.492, 39.914), tqc);
        KNNQuery knnQuery = new KNNQuery(
                HBaseConnector.getDataSetMeta("T10"), knnQueryCondition);

        try (SparkSession sparkSession =
                     SparkUtil.createSession(SingleKNNQueryProcess.class.getName())) {
            List<Trajectory> results = knnQuery.runBySpark(sparkSession);
            System.out.println("size: " + results.size());
//            for (Trajectory result : results) {
//                System.out.println(result);
//            }
            long end = System.currentTimeMillis();
            System.out.println("cost: " + (end - start) + "ms");
            results.forEach(r -> System.out.println("tid: " + r.getTid()));
            JSONObject jsonObject = TrajectoryMapper.mapTrajectoryListToGeoJson(results);
            IOUtil.writeStringToFile(
                    "/home/oge/ods/data/" + k + "1.json", jsonObject.toString());
            sparkSession.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
