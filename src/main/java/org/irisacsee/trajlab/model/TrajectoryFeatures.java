package org.irisacsee.trajlab.model;

import java.io.Serializable;
import java.time.ZonedDateTime;

/**
 * 轨迹特征
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class TrajectoryFeatures implements Serializable {
    private ZonedDateTime startTime;
    private ZonedDateTime endTime;
    private TrajectoryPoint startPoint;
    private TrajectoryPoint endPoint;
    private int pointNum;
    private MinimumBoundingBox mbr;
    private double speed;
    private double len;

    public TrajectoryFeatures(ZonedDateTime startTime, ZonedDateTime endTime, TrajectoryPoint startPoint,
                              TrajectoryPoint endPoint, int pointNum, MinimumBoundingBox mbr, double speed,
                              double len) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.startPoint = startPoint;
        this.endPoint = endPoint;
        this.pointNum = pointNum;
        this.mbr = mbr;
        this.speed = speed;
        this.len = len;
    }

    public void setStartTime(ZonedDateTime startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(ZonedDateTime endTime) {
        this.endTime = endTime;
    }

    public void setStartPoint(TrajectoryPoint startPoint) {
        this.startPoint = startPoint;
    }

    public void setEndPoint(TrajectoryPoint endPoint) {
        this.endPoint = endPoint;
    }

    public void setPointNum(int pointNum) {
        this.pointNum = pointNum;
    }

    public void setMbr(MinimumBoundingBox mbr) {
        this.mbr = mbr;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public void setLen(double len) {
        this.len = len;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public ZonedDateTime getEndTime() {
        return endTime;
    }

    public TrajectoryPoint getStartPoint() {
        return startPoint;
    }

    public TrajectoryPoint getEndPoint() {
        return endPoint;
    }

    public int getPointNum() {
        return pointNum;
    }

    public MinimumBoundingBox getMbr() {
        return mbr;
    }

    public double getSpeed() {
        return speed;
    }

    public double getLen() {
        return len;
    }

    @Override
    public String toString() {
        return "TrajFeatures{" +
                "startTime=" + startTime +
                ", endTime=" + endTime +
                ", startPoint=" + startPoint +
                ", endPoint=" + endPoint +
                ", pointNum=" + pointNum +
                ", mbr=" + mbr +
                ", speed=" + speed +
                ", len=" + len +
                '}';
    }
}
