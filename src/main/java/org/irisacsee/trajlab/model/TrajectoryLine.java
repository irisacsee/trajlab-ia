package org.irisacsee.trajlab.model;

import org.irisacsee.trajlab.util.DateUtil;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

import java.util.ArrayList;
import java.util.List;

/**
 * 轨迹序列
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class TrajectoryLine extends LineString {
    private List<TrajectoryPoint> trajectoryPoints;
    private Object userData = null;

    public TrajectoryLine(CoordinateSequence points, GeometryFactory factory) {
        super(points, factory);
    }

    public TrajectoryLine(CoordinateSequence points, GeometryFactory factory, List<TrajectoryPoint> pointList) {
        super(points, factory);
        this.trajectoryPoints = pointList;
    }

    public List<TrajectoryPoint> getTrajPoints() {
        return trajectoryPoints;
    }

    public void setTrajPoints(List<TrajectoryPoint> trajectoryPoints) {
        this.trajectoryPoints = trajectoryPoints;
    }

    @Override
    public Object getUserData() {
        return userData;
    }

    @Override
    public void setUserData(Object userData) {
        this.userData = userData;
    }

    @Override
    public Coordinate[] getCoordinates() {
        Coordinate[] coordinates = new Coordinate[trajectoryPoints.size()];
        List<Long> timestamp = new ArrayList<>();
        for (int i = 0; i < trajectoryPoints.size(); i++) {
            TrajectoryPoint trajectoryPoint = trajectoryPoints.get(i);
            coordinates[i] = new Coordinate(trajectoryPoint.getX(), trajectoryPoint.getY());
            long timeStamp = DateUtil.parseDateToTimeStamp(
                    trajectoryPoint.getTimestamp());
            timestamp.add(timeStamp);
        }
        userData = timestamp;
        return coordinates;
    }
}
