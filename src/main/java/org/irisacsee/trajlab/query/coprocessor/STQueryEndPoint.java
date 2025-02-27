package org.irisacsee.trajlab.query.coprocessor;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.irisacsee.trajlab.constant.DBConstant;
import org.irisacsee.trajlab.index.type.TimeLine;
import org.irisacsee.trajlab.model.BasePoint;
import org.irisacsee.trajlab.model.MinimumBoundingBox;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.model.TrajectoryMapper;
import org.irisacsee.trajlab.model.TrajectoryPoint;
import org.irisacsee.trajlab.query.coprocessor.autogenerated.QueryCondition;
import org.irisacsee.trajlab.serializer.ObjectSerializer;
import org.irisacsee.trajlab.store.HBaseConnector;
import org.irisacsee.trajlab.store.IndexTable;
import org.irisacsee.trajlab.util.DateUtil;
import org.irisacsee.trajlab.util.GeoUtil;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 协处理器
 * 
 * @author irisacsee
 * @since 2024/11/22
 */
public class STQueryEndPoint extends QueryCondition.QueryService
        implements Coprocessor, CoprocessorService {
    private final Logger logger = LoggerFactory.getLogger(STQueryEndPoint.class);
    private RegionCoprocessorEnvironment env;
    private Connection instance;

    @Override
    public void query(
            RpcController controller,
            QueryCondition.QueryRequest request,
            RpcCallback<QueryCondition.QueryResponse> done) {
        QueryCondition.QueryMethod queryOperation = request.getQueryOperation();
        List<QueryCondition.Range> rangeList = request.getRangeList();
        // 将Range List按照是否需要二次判断分组，并生成两个Scan
        List<MarkedScan> markedScans = null;
        try {
            markedScans = getTowScan(rangeList);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<QueryCondition.TrajectoryResult> trajectoryResults = new ArrayList<>();
        switch (queryOperation) {
            case ST: {
                QueryCondition.STQueryRequest stRequest = request.getSt();
                boolean filterBeforeLookFullRow = stRequest.getFilterBeforeLookFullRow();
                try {
                    // 扫描, 解析结果
                    for (MarkedScan markedScan : markedScans) {
                        Scan scan = markedScan.scan;
                        InternalScanner scanner = env.getRegion().getScanner(scan);
                        List<Cell> cells = new ArrayList<>();
                        boolean hasMore = scanner.next(cells) || !cells.isEmpty();
                        while (hasMore) {
                            Result result = Result.create(cells);
                            if (!markedScan.needFilter) {
                                if (!TrajectoryMapper.isMainIndexed(result)) {
                                    result = getMainIndexedResult(result);
                                }
                                trajectoryResults.add(buildTrajectoryResult(result));
                            } else {
                                // 如果当前索引是辅助索引，且不需要在回表查询之前作粗过滤，则先回表查询。
                                if (!TrajectoryMapper.isMainIndexed(result) && !filterBeforeLookFullRow) {
                                    result = getMainIndexedResult(result);
                                }
                                // 使用pos code, mbr等粗过滤
                                if (coarseFilter(result, stRequest)) {
                                    // 如果filterBeforeLookFullRow为真，则此时result仍然是辅助索引
                                    // 相当于先通过了初步过滤，现在要回表查询。
                                    if (!TrajectoryMapper.isMainIndexed(result)) {
                                        result = getMainIndexedResult(result);
                                    }
                                    // 使用point list作精过滤
                                    if (fineFilter(result, stRequest)) {
                                        trajectoryResults.add(buildTrajectoryResult(result));
                                    }
                                }
                            }
                            // 读取下一行
                            cells.clear();
                            hasMore = scanner.next(cells) || !cells.isEmpty();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }
            case KNN: {
                QueryCondition.KNNQueryRequest knn = request.getKnn();
                try {
                    // 扫描, 解析结果
                    for (MarkedScan markedScan : markedScans) {
                        Scan scan = markedScan.scan;
                        InternalScanner scanner = env.getRegion().getScanner(scan);
                        List<Cell> cells = new ArrayList<>();
                        boolean hasMore = scanner.next(cells) || !cells.isEmpty();
                        while (hasMore) {
                            Result result = Result.create(cells);
                            // 如果当前索引是辅助索引，且不需要在回表查询之前作粗过滤，则先回表查询。
                            if (!TrajectoryMapper.isMainIndexed(result)) {
                                result = getMainIndexedResult(result);
                            }
                            if (knnFilter(result, knn)) {
                                trajectoryResults.add(buildTrajectoryResult(result));
                            }
                            // 读取下一行
                            cells.clear();
                            hasMore = scanner.next(cells) || !cells.isEmpty();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }
            case SIMILAR: {
                QueryCondition.SimilarQueryRequest sim = request.getSim();
                try {
                    // 扫描, 解析结果
                    for (MarkedScan markedScan : markedScans) {
                        Scan scan = markedScan.scan;
                        InternalScanner scanner = env.getRegion().getScanner(scan);
                        List<Cell> cells = new ArrayList<>();
                        boolean hasMore = scanner.next(cells) || !cells.isEmpty();
                        while (hasMore) {
                            Result result = Result.create(cells);
                            // 如果当前索引是辅助索引，且不需要在回表查询之前作粗过滤，则先回表查询。
                            if (!TrajectoryMapper.isMainIndexed(result)) {
                                result = getMainIndexedResult(result);
                            }
                            if (simFilter(result, sim)) {
                                if (fineSimFilter(result, sim)) {
                                    trajectoryResults.add(buildTrajectoryResult(result));
                                }
                            }
                            // 读取下一行
                            cells.clear();
                            hasMore = scanner.next(cells) || !cells.isEmpty();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }
            case ACCOMPANY: {
                QueryCondition.AccompanyQueryRequest acc = request.getAcc();
                try {
                    // 扫描, 解析结果
                    for (MarkedScan markedScan : markedScans) {
                        Scan scan = markedScan.scan;
                        InternalScanner scanner = env.getRegion().getScanner(scan);
                        List<Cell> cells = new ArrayList<>();
                        boolean hasMore = scanner.next(cells) || !cells.isEmpty();
                        while (hasMore) {
                            Result result = Result.create(cells);
                            // 如果当前索引是辅助索引，且不需要在回表查询之前作粗过滤，则先回表查询。
                            if (!TrajectoryMapper.isMainIndexed(result)) {
                                result = getMainIndexedResult(result);
                            }
                            if (accFilter(result, acc)) {
                                if (fineAccFilter(result, acc)) {
                                    trajectoryResults.add(buildTrajectoryResult(result));
                                }
                            }
                            // 读取下一行
                            cells.clear();
                            hasMore = scanner.next(cells) || !cells.isEmpty();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }
        }

        QueryCondition.QueryResponse response =
                QueryCondition.QueryResponse.newBuilder().addAllList(trajectoryResults).build();
        done.run(response);
    }

    /**
     * Get a scan list with at most size 2 to scan ranges that need / don't need server-side column
     * filter respectively.
     *
     * @return The scan object at offset 0 will read all records that <strong>absolutely</strong> meet
     *     the query request, the other object at offset 1 will read all records that
     *     <strong>may</strong> meet the query request according to the row key coding strategy.
     * @throws IOException
     */
    private List<MarkedScan> getTowScan(List<QueryCondition.Range> rangeList) throws IOException {
        List<MultiRowRangeFilter.RowRange> rowRangeListConfirmed = new ArrayList<>(rangeList.size());
        List<MultiRowRangeFilter.RowRange> rowRangeListSuspected = new ArrayList<>(rangeList.size());
        for (QueryCondition.Range range : rangeList) {
            List<MultiRowRangeFilter.RowRange> listToAdd =
                    range.getContained() ? rowRangeListConfirmed : rowRangeListSuspected;
            listToAdd.add(
                    new MultiRowRangeFilter.RowRange(
                            range.getStart().toByteArray(), true, range.getEnd().toByteArray(), false));
        }
        List<MarkedScan> markedScans = new LinkedList<>();
        if (!rowRangeListConfirmed.isEmpty()) {
            Scan scan0 = buildScan();
            scan0.addColumn(DBConstant.COLUMN_FAMILY, DBConstant.PTR_QUALIFIER);
            scan0.setFilter(new MultiRowRangeFilter(rowRangeListConfirmed));
            markedScans.add(new MarkedScan(scan0, false));
        }
        if (!rowRangeListSuspected.isEmpty()) {
            Scan scan1 = buildScan();
            scan1.addColumn(DBConstant.COLUMN_FAMILY, DBConstant.PTR_QUALIFIER);
            scan1.setFilter(new MultiRowRangeFilter(rowRangeListSuspected));
            markedScans.add(new MarkedScan(scan1, true));
        }
        return markedScans;
    }

    /** 获取含以下列的scan: mbr, start, end, plist, mo_id, traj_id, ptr */
    protected Scan buildScan() {
        Scan scan = new Scan();
        scan.addColumn(DBConstant.COLUMN_FAMILY, DBConstant.MBR_QUALIFIER);
        scan.addColumn(DBConstant.COLUMN_FAMILY, DBConstant.START_POINT_QUALIFIER);
        scan.addColumn(DBConstant.COLUMN_FAMILY, DBConstant.END_POINT_QUALIFIER);
        scan.addColumn(DBConstant.COLUMN_FAMILY, DBConstant.TRAJ_POINTS_QUALIFIER);
        scan.addColumn(DBConstant.COLUMN_FAMILY, DBConstant.OBJECT_ID_QUALIFIER);
        scan.addColumn(DBConstant.COLUMN_FAMILY, DBConstant.TRAJECTORY_ID_QUALIFIER);
        scan.addColumn(DBConstant.COLUMN_FAMILY, DBConstant.PTR_QUALIFIER);
        return scan;
    }

    /** 基于辅助索引表的行回表，获取主索引表的行： 先从Instance获取数据集的名称，再找到coreIndex */
    private Result getMainIndexedResult(Result result) throws IOException {
        return new IndexTable(HBaseConnector.getDataSetMeta(IndexTable.extractDataSetName(
                env.getRegionInfo().getTable().getNameAsString())).getMainIndexMeta())
                .get(new Get(result.getValue(DBConstant.COLUMN_FAMILY, DBConstant.PTR_QUALIFIER)));
    }

    /**
     * 基于result内以单独列存储的信息作粗过滤, 例如:mbr, start_time, end_time等.
     *
     * @param result
     * @param queryRequest
     * @return
     */
    protected boolean coarseFilter(Result result, QueryCondition.STQueryRequest queryRequest) {
        WKTReader wktReader = new WKTReader();
        boolean spatialValidate = false;
        boolean temporalValidate = false;
        // 利用MBR对QueryRequest中的空间约束作初步判断
        if (queryRequest.hasSpatialQueryWindow()) {
            try {
                Geometry queryGeom = wktReader.read(queryRequest.getSpatialQueryWindow().getWkt());
                MinimumBoundingBox mbr = TrajectoryMapper.getTrajectoryMBR(result);
                if (queryRequest.getSpatialQueryType() == QueryCondition.QueryType.CONTAIN) {
                    spatialValidate = queryGeom.contains(mbr.toPolygon(4326));
                } else {
                    spatialValidate = queryGeom.intersects(mbr.toPolygon(4326));
                }
            } catch (ParseException | IOException e) {
                e.printStackTrace();
            }
        } else {
            spatialValidate = true;
        }
        if (queryRequest.hasTemporalQueryWindows()) {
            List<QueryCondition.TemporalQueryWindow> temporalQueryWindowList =
                    queryRequest.getTemporalQueryWindows().getTemporalQueryWindowList();
            // start and end time of the traj
            TimeLine trajTimeLine = TrajectoryMapper.getTrajectoryTimeLine(result);
            for (QueryCondition.TemporalQueryWindow temporalQueryWindow : temporalQueryWindowList) {
                TimeLine queryTimeLine =
                        new TimeLine(
                                DateUtil.timeToZonedTime(temporalQueryWindow.getStartMs()),
                                DateUtil.timeToZonedTime(temporalQueryWindow.getEndMs()));
                if (queryRequest.getTemporalQueryType() == QueryCondition.QueryType.CONTAIN) {
                    temporalValidate = temporalValidate || queryTimeLine.contain(trajTimeLine);
                } else {
                    temporalValidate = temporalValidate || queryTimeLine.intersect(trajTimeLine);
                }
            }
        } else {
            temporalValidate = true;
        }
        return spatialValidate && temporalValidate;
    }

    /**
     * 根据需要配置具体的精过滤条件, 对于最常用的时间\空间条件, 已提供了如下开箱即用的精过滤方法:
     *
     * @see STQueryEndPoint#spatialFineFilter(Result, QueryCondition.STQueryRequest)
     */
    protected boolean fineFilter(Result result, QueryCondition.STQueryRequest stRequest)
            throws IOException {
        boolean validate = true;
        if (stRequest.hasSpatialQueryWindow()) {
            validate = spatialFineFilter(result, stRequest);
        }
        return validate;
    }

    /**
     * Fine filter based on trajectory line string.
     *
     * @param result A main indexed trajectory row.
     * @param stRequest To get spatial query polygon and spatial query type.
     * @return Whether the result trajectory satisfies queryCondition.
     */
    protected boolean spatialFineFilter(Result result, QueryCondition.STQueryRequest stRequest)
            throws IOException {
        Trajectory trajectory = TrajectoryMapper.mainRowToTrajectory(result);
        WKTReader wktReader = new WKTReader();
        try {
            Geometry queryGeom = wktReader.read(stRequest.getSpatialQueryWindow().getWkt());
            if (stRequest.getSpatialQueryType() == QueryCondition.QueryType.CONTAIN) {
                return queryGeom.contains(trajectory.getLineString());
            }
            return queryGeom.intersects(trajectory.getLineString());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return false;
    }

    protected boolean timeFilter(
            Result result, QueryCondition.KNNQueryRequest knnQueryRequest) {
        if (!knnQueryRequest.hasTemporalQueryWindow()) return true;
        QueryCondition.TemporalQueryWindow temporalQueryWindow = knnQueryRequest.getTemporalQueryWindow();
        TimeLine trajTimeLine = TrajectoryMapper.getTrajectoryTimeLine(result);
        TimeLine queryTimeLine =
                new TimeLine(
                        DateUtil.timeToZonedTime(temporalQueryWindow.getStartMs()),
                        DateUtil.timeToZonedTime(temporalQueryWindow.getEndMs()));
        return queryTimeLine.intersect(trajTimeLine);
    }
    protected boolean timeFilter(
            Result result, QueryCondition.SimilarQueryRequest similarQueryRequest) {
        if (!similarQueryRequest.hasTemporalQueryWindow()) return true;
        QueryCondition.TemporalQueryWindow temporalQueryWindow = similarQueryRequest.getTemporalQueryWindow();
        TimeLine trajTimeLine = TrajectoryMapper.getTrajectoryTimeLine(result);
        TimeLine queryTimeLine =
                new TimeLine(
                        DateUtil.timeToZonedTime(temporalQueryWindow.getStartMs()),
                        DateUtil.timeToZonedTime(temporalQueryWindow.getEndMs()));
        return queryTimeLine.intersect(trajTimeLine);
    }

    protected boolean knnFilter(Result result, QueryCondition.KNNQueryRequest knnQueryRequest)
            throws IOException {
        MinimumBoundingBox mbr = TrajectoryMapper.getTrajectoryMBR(result);
        Geometry mbrPolygon = GeoUtil.createEnvelopeGeometry(mbr);
//    Polygon mbrPolygon = mbr.toPolygon(4326);
        double maxDis = knnQueryRequest.getDistance();
        if (knnQueryRequest.hasPoint()) {
            // 使用DistanceOp计算最短距离
            BasePoint point = (BasePoint) ObjectSerializer.deserializeObject(
                    knnQueryRequest.getPoint().toByteArray(), BasePoint.class);
            double distance = GeoUtil.nearDistanceOp(point, mbrPolygon);
            return distance <= maxDis && timeFilter(result, knnQueryRequest);
        } else {
            Trajectory trajectory =
                    (Trajectory)
                            ObjectSerializer.deserializeObject(
                                    knnQueryRequest.getTrajectory().toByteArray(), Trajectory.class);
            Trajectory trajectoryFromResult = TrajectoryMapper.getTrajectoryFromResult(result);
            if(trajectoryFromResult.equals(trajectory)) return false;
            double distance = Double.MAX_VALUE;
            for (TrajectoryPoint TrajectoryPoint : trajectory.getPoints()) {
                double pointDis = GeoUtil.nearDistanceOp(TrajectoryPoint, mbrPolygon);
                distance = Math.min(distance, pointDis);
            }
            return distance <= maxDis && timeFilter(result, knnQueryRequest);
        }
    }

    protected boolean simFilter(Result result, QueryCondition.SimilarQueryRequest similarQueryRequest)
            throws IOException {
        MinimumBoundingBox mbr = TrajectoryMapper.getTrajectoryMBR(result);
        double maxDis = similarQueryRequest.getDistance();
        Trajectory centralTrajectory =
                (Trajectory)
                        ObjectSerializer.deserializeObject(
                                similarQueryRequest.getTrajectory().toByteArray(), Trajectory.class);
        Trajectory trajectoryFromResult = TrajectoryMapper.getTrajectoryFromResult(result);
        if(trajectoryFromResult.equals(centralTrajectory)) return false;
        Envelope envelopeInternal = centralTrajectory.getLineString().getEnvelopeInternal();
        envelopeInternal.expandBy(maxDis);
        TrajectoryPoint startPoint = centralTrajectory.getTrajectoryFeatures().getStartPoint();
        TrajectoryPoint endPoint = centralTrajectory.getTrajectoryFeatures().getEndPoint();
        if (!envelopeInternal.contains(mbr)) return false;
        Tuple2<TrajectoryPoint, TrajectoryPoint> trajectorySEPoint =
                TrajectoryMapper.getTrajectorySEPoint(result);
        double distance1 = GeoUtil.getEuclideanDistanceKM(startPoint, trajectorySEPoint._1);
        double distance2 = GeoUtil.getEuclideanDistanceKM(endPoint, trajectorySEPoint._2);

        return GeoUtil.getDegreeFromKm(distance1) <= maxDis
                && GeoUtil.getDegreeFromKm(distance2) <= maxDis
                && timeFilter(result, similarQueryRequest);
    }

    protected boolean accFilter(
            Result result, QueryCondition.AccompanyQueryRequest accompanyQueryRequest)
            throws IOException {

        MinimumBoundingBox mbr = TrajectoryMapper.getTrajectoryMBR(result);
        TimeLine tLine = TrajectoryMapper.getTrajectoryTimeLine(result);

        Trajectory centralTrajectory =
                (Trajectory)
                        ObjectSerializer.deserializeObject(
                                accompanyQueryRequest.getTrajectory().toByteArray(), Trajectory.class);
        Trajectory trajectoryFromResult = TrajectoryMapper.getTrajectoryFromResult(result);
        if (trajectoryFromResult.equals(centralTrajectory)) return false;
        int startPointCount = accompanyQueryRequest.getStartPoint();
        int k = accompanyQueryRequest.getK();
        //空间缓冲区
        List<TrajectoryPoint> TrajectoryPoints =
                centralTrajectory.getPoints().subList(startPointCount, startPointCount + k);
        MinimumBoundingBox minimumBoundingBox = GeoUtil.calMinimumBoundingBox(TrajectoryPoints);
        minimumBoundingBox.expandBy(accompanyQueryRequest.getDistance());
        //时间缓冲区
        long timeInterval = accompanyQueryRequest.getTimeInterval();
        TimeLine timeLine =
                new TimeLine(
                        TrajectoryPoints.get(0).getTimestamp().minusNanos(timeInterval * 1000000),
                        TrajectoryPoints.get(TrajectoryPoints.size() - 1).getTimestamp().plusNanos(timeInterval * 1000000));
        return mbr.isIntersects(minimumBoundingBox) && tLine.intersect(timeLine);
    }

    protected boolean fineSimFilter(
            Result result, QueryCondition.SimilarQueryRequest similarQueryRequest) throws IOException {
        double maxDis = similarQueryRequest.getDistance();
        Trajectory resultTrajectory = TrajectoryMapper.getTrajectoryFromResult(result);
        Trajectory centralTrajectory =
                (Trajectory)
                        ObjectSerializer.deserializeObject(
                                similarQueryRequest.getTrajectory().toByteArray(), Trajectory.class);
        double dfd = GeoUtil.calculateDiscreteFrechetDistance(
                resultTrajectory.getLineString(), centralTrajectory.getLineString());
        return GeoUtil.getDegreeFromKm(dfd) <= maxDis;
    }

    protected boolean fineAccFilter(
            Result result, QueryCondition.AccompanyQueryRequest accompanyQueryRequest)
            throws IOException {
        Trajectory trajectory = TrajectoryMapper.mainRowToTrajectory(result);
        TimeLine tLine = TrajectoryMapper.getTrajectoryTimeLine(result);

        Trajectory centralTrajectory =
                (Trajectory)
                        ObjectSerializer.deserializeObject(
                                accompanyQueryRequest.getTrajectory().toByteArray(), Trajectory.class);
        int startPointCount = accompanyQueryRequest.getStartPoint();
        int k = accompanyQueryRequest.getK();
        List<TrajectoryPoint> TrajectoryPoints =
                centralTrajectory.getPoints().subList(startPointCount, startPointCount + k);
        double distance = accompanyQueryRequest.getDistance();
        long timeInterval = accompanyQueryRequest.getTimeInterval();

        for (TrajectoryPoint TrajectoryPoint : TrajectoryPoints) {
            Geometry buffer = TrajectoryPoint.buffer(distance);
            TimeLine timeLine =
                    new TimeLine(
                            TrajectoryPoint.getTimestamp().minusNanos(timeInterval * 1000000),
                            TrajectoryPoint.getTimestamp().plusNanos(timeInterval * 1000000));
            if (!buffer.intersects(trajectory.getLineString()) || !timeLine.intersect(tLine)) return false;
        }
        return true;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            logger.info("STQueryEndPoint start");
            this.env = (RegionCoprocessorEnvironment) env;
            instance = HBaseConnector.getConnection();
        } else {
            throw new CoprocessorException("Failed to init coprocessor env.");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        HBaseConnector.closeConnection();
        logger.warn("STQueryEndPoint is unregistered, running stop() hook!");
    }

    @Override
    public Service getService() {
        return this;
    }

    static class MarkedScan {

        Scan scan;
        boolean needFilter;

        MarkedScan(Scan scan, boolean needFilter) {
            this.scan = scan;
            this.needFilter = needFilter;
        }
    }

    private QueryCondition.TrajectoryResult buildTrajectoryResult(Result result) {
        return QueryCondition.TrajectoryResult.newBuilder()
                .setRowkey(ByteString.copyFrom(result.getRow()))
                .setTrajPointList(
                        ByteString.copyFrom(
                                TrajectoryMapper.getByteArrayByQualifier(
                                        result, DBConstant.TRAJ_POINTS_QUALIFIER)))
                .setObjectId(
                        ByteString.copyFrom(
                                TrajectoryMapper.getByteArrayByQualifier(
                                        result, DBConstant.OBJECT_ID_QUALIFIER)))
                .setTid(
                        ByteString.copyFrom(
                                TrajectoryMapper.getByteArrayByQualifier(
                                        result, DBConstant.TRAJECTORY_ID_QUALIFIER)))
                .build();
    }
}

