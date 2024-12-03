package org.irisacsee.trajlab.model;

import org.irisacsee.trajlab.util.DateUtil;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

public class TrajectoryPoint extends BasePoint {
    private String pid;
    private String timeString;
    private ZonedDateTime timestamp;
    private Map<String, Object> extendedValues;

    public TrajectoryPoint(ZonedDateTime timestamp, double lng, double lat) {
        super(lng, lat);
        this.pid = null;
        this.timestamp = timestamp;
        this.extendedValues = null;
    }

    public TrajectoryPoint(String timeString, double lng, double lat) {
        super(lng, lat);
        this.timeString = timeString;
    }

    public TrajectoryPoint(String id, ZonedDateTime timestamp, double lng, double lat) {
        super(lng, lat);
        this.pid = id;
        this.timestamp = timestamp;
        this.extendedValues = null;
    }

    public TrajectoryPoint(String id, String timeString, double lng, double lat) {
        super(lng, lat);
        this.pid = id;
        this.timeString = timeString;
    }

    public TrajectoryPoint(String id, ZonedDateTime timestamp, double lng, double lat,
                           Map<String, Object> extendedValues) {
        super(lng, lat);
        this.pid = id;
        this.timestamp = timestamp;
        this.extendedValues = extendedValues;
    }

    public TrajectoryPoint(String id, String timeString, double lng, double lat,
                           Map<String, Object> extendedValues) {
        super(lng, lat);
        this.pid = id;
        this.timeString = timeString;
        this.extendedValues = extendedValues;
    }

    public void setTimeString(String timeString) {
        this.timeString = timeString;
    }

    public void setTimestamp(ZonedDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getPid() {
        return pid;
    }

    public ZonedDateTime getTimestamp() {
        if (timestamp == null) {
            timestamp = DateUtil.parseDate(timeString);
        }
        return timestamp;
    }

    public String getTimestampString() {
        if(timeString == null){
            timeString = DateUtil.format(timestamp, DateUtil.DEFAULT_FORMAT);
        }
        return timeString;
    }

    public Map<String, Object> getExtendedValues() {
        return this.extendedValues == null ? null : this.extendedValues;
    }

    public void setExtendedValues(Map<String, Object> extendedValues) {
        this.extendedValues = extendedValues;
    }

    public Object getExtendedValue(String key) {
        return this.extendedValues == null ? null : this.extendedValues.get(key);
    }

    public void setExtendedValue(String key, Object value) {
        if (this.extendedValues == null) {
            this.extendedValues = new HashMap<>();
        }
        this.extendedValues.put(key, value);
    }

    public void removeExtendedValue(String key) {
        if (this.extendedValues != null) {
            this.extendedValues.remove(key);
        }
    }


    public String toString() {
        return "TrajPoint{pid='" + this.pid + '\'' + ", longitude=" + this.getX() + ", latitude="
                + this.getY() + ", timeStamp=" + this.getTimestamp() + ", extendedValues="
                + this.extendedValues + '}';
    }
    public String getPointSequence() {
        return String.format(
                "(%s,%s,%s,%s)",
                this.pid,
                this.timestamp,
                this.getX(),
                this.getY());
    }
}
