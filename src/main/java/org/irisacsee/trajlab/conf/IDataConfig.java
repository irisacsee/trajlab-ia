package org.irisacsee.trajlab.conf;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.irisacsee.trajlab.conf.enums.DataType;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/11/14
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME
)
@JsonSubTypes({@JsonSubTypes.Type(
        value = TrajectoryConfig.class,
        name = "trajectory"
), @JsonSubTypes.Type(
        value = TrajectoryPointConfig.class,
        name = "traj_point"
)})
public interface IDataConfig extends Serializable {
    DataType getDataType();
}
