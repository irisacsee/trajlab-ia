package org.irisacsee.trajlab.conf;

import org.irisacsee.trajlab.conf.enums.BasicDataType;
import org.irisacsee.trajlab.conf.enums.DataType;

import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class TrajectoryPointConfig implements IDataConfig {

    Field pointId;

    Field lat;

    Field lng;

    Field time;

    List<Mapping> trajPointMetas;

    public TrajectoryPointConfig() {
        this.pointId = new Field("traj_point_id", BasicDataType.STRING);
        this.lat = new Field("lat", BasicDataType.DOUBLE);
        this.lng = new Field("lng", BasicDataType.DOUBLE);
        this.time = new Field("time", BasicDataType.DATE);
    }

    public Field getPointId() {
        return this.pointId;
    }

    public Field getLat() {
        return this.lat;
    }

    public Field getLng() {
        return this.lng;
    }

    public Field getTime() {
        return this.time;
    }

    public List<Mapping> getTrajPointMetas() {
        return this.trajPointMetas;
    }

    public DataType getDataType() {
        return DataType.TRAJ_POINT;
    }
}