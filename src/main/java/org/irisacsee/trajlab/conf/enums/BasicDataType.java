package org.irisacsee.trajlab.conf.enums;

import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public enum BasicDataType implements Serializable {
    STRING("String"),
    INT("Integer"),
    LONG("Long"),
    DOUBLE("Double"),
    DATE("Date"),
    TIME_STRING("TimeString"),
    TIME_LONG("TimeLong"),
    TIMESTAMP("Timestamp"),
    LIST("List");

    private String typeName;

    BasicDataType(String typeName) {
        this.typeName = typeName;
    }
    @JsonValue
    public final String getType() {
        return this.typeName;
    }
}
