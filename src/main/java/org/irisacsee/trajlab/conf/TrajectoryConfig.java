package org.irisacsee.trajlab.conf;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.irisacsee.trajlab.conf.enums.BasicDataType;
import org.irisacsee.trajlab.conf.enums.DataType;
import org.irisacsee.trajlab.model.TrajectoryFeatures;

import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class TrajectoryConfig implements IDataConfig {
    @JsonProperty
    private Field trajId;
    @JsonProperty
    private Field objectId;
    @JsonProperty
    private Field trajLog;
    @JsonProperty
    private Field timeList;
    @JsonProperty
    private TrajectoryPointConfig trajPointConfig;
    @JsonProperty
    private TrajectoryFeatures trajectoryFeatures;
    @JsonProperty
    private List<Mapping> trajectoryMetas;
    @JsonProperty
    private String attrIndexable;
    @JsonProperty
    private String pointIndex;
    @JsonProperty
    private String listIndex;

    public TrajectoryConfig() {
        this.trajId = new Field("trajectory_id", BasicDataType.STRING);
        this.objectId = new Field("object_id", BasicDataType.STRING);
        this.trajLog = new Field("traj_list", BasicDataType.STRING);
        this.timeList = new Field("time_list", BasicDataType.STRING);
        this.trajPointConfig = new TrajectoryPointConfig();
        this.pointIndex = "";
        this.listIndex = "";
    }

    public Field getTrajId() {
        return this.trajId;
    }

    public Field getObjectId() {
        return this.objectId;
    }

    public TrajectoryFeatures getTrajectoryFeatures() {
        return this.trajectoryFeatures;
    }

    public List<Mapping> getTrajectoryMetas() {
        return this.trajectoryMetas;
    }

    public TrajectoryPointConfig getTrajPointConfig() {
        return trajPointConfig;
    }

    public Field getTrajLog() {
        return this.trajLog;
    }

    public DataType getDataType() {
        return DataType.TRAJECTORY;
    }

    public String isIndexable() {
        return this.attrIndexable;
    }

    public String getPointIndex() {
        return this.pointIndex;
    }

    public String getListIndex() {
        return this.listIndex;
    }

    public Field getTimeList() {
        return timeList;
    }
}
