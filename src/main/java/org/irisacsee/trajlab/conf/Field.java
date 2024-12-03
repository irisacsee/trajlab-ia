package org.irisacsee.trajlab.conf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.irisacsee.trajlab.conf.enums.BasicDataType;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class Field implements Serializable {
    private String sourceName;
    private BasicDataType BasicDataType;
    private int index;
    private String format;
    private String zoneId;
    private String unit;

    @JsonCreator
    public Field(@JsonProperty("sourceName") String sourceName, @JsonProperty("dataType") BasicDataType BasicDataType, @JsonProperty("whu/edu/cn/trajlab/db/index") @JsonInclude(JsonInclude.Include.NON_NULL) int index, @JsonProperty("format") @JsonInclude(JsonInclude.Include.NON_NULL) String format, @JsonProperty("zoneId") @JsonInclude(JsonInclude.Include.NON_NULL) String zoneId, @JsonProperty("unit") @JsonInclude(JsonInclude.Include.NON_NULL) String unit) {
        this.sourceName = sourceName;
        this.BasicDataType = BasicDataType;
        this.index = index;
        this.format = format;
        this.zoneId = zoneId;
        this.unit = unit;
    }

    public Field() {
    }

    public Field(String sourceName, BasicDataType BasicDataType) {
        this.sourceName = sourceName;
        this.BasicDataType = BasicDataType;
    }

    public Field(String sourceName, BasicDataType BasicDataType, int index) {
        this.sourceName = sourceName;
        this.BasicDataType = BasicDataType;
        this.index = index;
    }

    public String getSourceName() {
        return this.sourceName;
    }

    public BasicDataType getBasicDataType() {
        return this.BasicDataType;
    }

    public int getIndex() {
        return this.index;
    }

    public String getFormat() {
        return this.format;
    }

    public String getZoneId() {
        return this.zoneId;
    }

    public String getUnit() {
        return this.unit;
    }
}
