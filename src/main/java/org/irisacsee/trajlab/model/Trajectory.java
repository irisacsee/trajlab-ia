package org.irisacsee.trajlab.model;

import org.irisacsee.trajlab.util.DateUtil;
import org.irisacsee.trajlab.util.GeoUtil;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import scala.Tuple2;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 轨迹类
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class Trajectory implements Serializable {
    private String tid;
    private String oid;
    private List<TrajectoryPoint> points;
    private TrajectoryFeatures features;
    private boolean updateFeatures = true;
    private boolean updateLineString = true;
    private boolean updatePointListId = true;
    private Map<String, Object> extendedValues;
    private TrajectoryLine lineString;

    public Trajectory() {}

    public Trajectory(
            String tid,
            String oid,
            List<TrajectoryPoint> points,
            TrajectoryFeatures features) {
        this.tid = tid;
        this.oid = oid;
        this.points = points;
        this.features = features;
        this.updateFeatures = false;
    }

    public Trajectory(
            String tid,
            String oid,
            List<TrajectoryPoint> points,
            TrajectoryFeatures features,
            Map<String, Object> extendedValues) {
        this.tid = tid;
        this.oid = oid;
        this.points = points;
        this.features = features;
        this.extendedValues = extendedValues;
        this.updateFeatures = false;
    }

    public Trajectory(
            String tid,
            String oid,
            List<TrajectoryPoint> points,
            Map<String, Object> extendedValues) {
        this.tid = tid;
        this.oid = oid;
        this.points = points;
        this.extendedValues = extendedValues;
    }

    public Trajectory(String tid, String oid, List<TrajectoryPoint> points) {
        this.tid = tid;
        this.oid = oid;
        this.points = points;
    }

    public Trajectory(
            String tid, String oid, List<TrajectoryPoint> points, boolean genPid) {
        this.tid = tid;
        this.oid = oid;
        this.points = points;
        if (genPid) {
            updatePointListId();
        }
    }

    public boolean isUpdateFeatures() {
        return updateFeatures;
    }

    public void addPoint(TrajectoryPoint point) {
        if (points == null || points.isEmpty()) {
            points = new ArrayList<>();
        }
        points.add(point);
        updateFeatures = true;
        updateLineString = true;
        updatePointListId = true;
    }

    public void setOid(String oid) {
        this.oid = oid;
    }

    public void setPoints(List<TrajectoryPoint> points) {
        this.points = points;
    }

    public void setFeatures(TrajectoryFeatures features) {
        this.features = features;
        this.updateFeatures = false;
    }

    public void setTid(String tid) {
        this.tid = tid;
    }

    public void addPoints(List<TrajectoryPoint> points) {
        if (this.points == null || this.points.isEmpty()) {
            this.points = new ArrayList<>();
        }

        this.points.addAll(points);
        updateFeatures = true;
        updateLineString = true;
        updatePointListId = true;
    }

    public LineString getLineString() {
        if (updateLineString) {
            updateLineString();
        }
        return lineString;
    }

    public LineString getLineStringAsDate() {
        if (updateLineString) {
            updateLineStringDate();
        }
        return lineString;
    }

    public String getTid() {
        return tid;
    }

    public String getOid() {
        return oid;
    }

    public List<TrajectoryPoint> getPoints() {
        return points;
    }

    public List<TrajectoryPoint> getUpdatedPointList() {
        if (updatePointListId) {
            updatePointListId();
        }
        return points;
    }

    public Map<String, Object> getExtendedValues() {
        return extendedValues;
    }

    public void setExtendedValues(Map<String, Object> extendedValues) {
        this.extendedValues = extendedValues;
    }

    public void setSRID(int srid) {
        this.getLineString().setSRID(srid);
    }

    public int getSRID() {
        return getLineString().getSRID();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Trajectory that = (Trajectory) o;
        return Objects.equals(tid, that.tid) && Objects.equals(oid, that.oid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tid, oid);
    }

    public TrajectoryFeatures getTrajectoryFeatures() {
        if (updateFeatures && points != null && !points.isEmpty()) {
            updateFeature();
            updateFeatures = false;
        }

        return features;
    }

    private void updateFeature() {
        points.sort((o1, o2) -> (int) (o1.getTimestamp().toEpochSecond() - o2.getTimestamp().toEpochSecond()));
        int size = points.size();
        ZonedDateTime startTime = points.get(0).getTimestamp();
        ZonedDateTime endTime = points.get(size - 1).getTimestamp();
        Tuple2<Double, MinimumBoundingBox> mbRandLengthOfList = getMBRandLengthOfList(points);
        MinimumBoundingBox mbr = mbRandLengthOfList._2;
        Double length = mbRandLengthOfList._1;
        double hour = (double) (endTime.toEpochSecond() - startTime.toEpochSecond()) / 60.0 / 60.0;
        double speed = length / hour;
        features = new TrajectoryFeatures(startTime, endTime, points.get(0), points.get(size - 1), size, mbr, speed, length);
    }

    private void updateLineString() {
        if (points != null) {
            int srid = lineString == null ? 4326 : lineString.getSRID();
            ArrayList<Long> list = new ArrayList<>();
            lineString = new TrajectoryLine(new CoordinateArraySequence(
                    points.stream()
                            .map(gpsPoint -> {
                                list.add(DateUtil.parseDateToTimeStamp(gpsPoint.getTimestamp()));
                                return new Coordinate(gpsPoint.getLng(), gpsPoint.getLat());})
                            .toArray(Coordinate[]::new)),
                    new GeometryFactory(new PrecisionModel(), srid),
                    points);
            updateLineString = false;
            lineString.setUserData(list);
        }
    }

    private void updateLineStringDate() {
        if (points != null) {
            int srid = lineString == null ? 4326 : lineString.getSRID();
            lineString = new TrajectoryLine(new CoordinateArraySequence(
                    points.stream()
                            .map(gpsPoint -> new Coordinate(
                                    gpsPoint.getLng(), gpsPoint.getLat(),
                                    DateUtil.parseDateToTimeStamp(gpsPoint.getTimestamp())))
                            .toArray(Coordinate[]::new)),
                    new GeometryFactory(new PrecisionModel(), srid),
                    points);
            updateLineString = false;
        }
    }

    private void updatePointListId() {
        for (int i = 0; i < points.size(); ++i) {
            points.get(i).setPid(String.valueOf(i));
        }
    }

    public boolean isIntersect(Trajectory otherTrajectory) {
        LineString otherLine = otherTrajectory.getLineString();
        return otherLine != null && otherLine.getNumPoints() != 0
                ? getLineString().intersects(otherLine)
                : false;
    }

    public boolean isPassPoint(Point point, double distance) {
        if (getLineString() != null && point != null) {
            double degree = GeoUtil.getDegreeFromKm(distance);
            return getLineString().intersects(point.buffer(degree));
        } else {
            return false;
        }
    }

    public double getPointListsLength(List<TrajectoryPoint> trajList) {
        double len = 0.0;
        for (int i = 1; i < trajList.size(); i++) {
            len += GeoUtil.getEuclideanDistanceKM(
                    trajList.get(i - 1).getCentroid(), trajList.get(i).getCentroid());
        }
        return len;
    }

    public Tuple2<Double, MinimumBoundingBox> getMBRandLengthOfList(List<TrajectoryPoint> plist) {
        Iterator<TrajectoryPoint> iter = plist.iterator();
        double length = 0.0;
        double minLat = Double.MAX_VALUE;
        double minLng = Double.MAX_VALUE;
        double maxLat = Double.MIN_VALUE;
        double maxLng = Double.MIN_VALUE;
        TrajectoryPoint prePoint = null;
        while (iter.hasNext()) {
            TrajectoryPoint p = iter.next();
            if (p != null) {
                maxLng = Math.max(maxLng, p.getLng());
                minLat = Math.min(minLat, p.getLat());
                minLng = Math.min(minLng, p.getLng());
                maxLat = Math.max(maxLat, p.getLat());
                if (prePoint != null) {
                    length += GeoUtil.getEuclideanDistanceKM(prePoint, p);
                }
                prePoint = p;
            }
        }
        MinimumBoundingBox mbr = new MinimumBoundingBox(minLng, minLat, maxLng, maxLat);
        return new Tuple2<>(length, mbr);
    }

    public Polygon buffer(double distance) {
        if (getLineString() == null) {
            return null;
        } else {
            double degree = GeoUtil.getDegreeFromKm(distance);
            return (Polygon) getLineString().buffer(degree);
        }
    }

    public Polygon convexHull() {
        return getLineString() == null ? null : (Polygon) getLineString().convexHull();
    }

    @Override
    public String toString() {
        return "Trajectory{"
                + "trajectoryID='"
                + tid
                + '\''
                + ", objectID='"
                + oid
                + '\''
                + ", trajectoryFeatures="
                + features
                + '}';
    }

    public static class Schema {

        public static final String TRAJECTORY_ID = "trajectory_id";
        public static final String OBJECT_ID = "object_id";
        public static final String TRAJ_POINTS = "traj_points";
        public static final String MBR = "whu/edu/cn/trajlab/base/mbr";
        public static final String START_TIME = "start_time";
        public static final String END_TIME = "end_time";
        public static final String START_POSITION = "start_position";
        public static final String END_POSITION = "end_position";
        public static final String POINT_NUMBER = "point_number";
        public static final String SPEED = "speed";
        public static final String LENGTH = "length";
        public static final String SIGNATURE = "signature";
        public static final String PTR = "PTR";
        public static final String EXT_VALUES = "extendedValues";

        public Schema() {}

        public static Set<String> defaultNameSet() throws IllegalAccessException {
            Set<String> defaultNames = new HashSet<>();
            Class<Schema> clazz = Schema.class;
            Field[] fields = clazz.getFields();
            for (Field field : fields) {
                defaultNames.add(field.get(clazz).toString());
            }
            return defaultNames;
        }
    }
}
