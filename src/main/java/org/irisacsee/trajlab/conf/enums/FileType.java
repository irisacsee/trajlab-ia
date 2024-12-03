package org.irisacsee.trajlab.conf.enums;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author xuqi
 * @date 2023/11/20
 */
public enum FileType {
    csv("csv"),
    geojson("geojson"),
    wkt("wkt"),
    kml("kml"),
    shp("shp"),
    parquet("parquet");
    private final String fileType;

    FileType(String fileType) {
        this.fileType = fileType;
    }

    @JsonValue
    public String getFileTypeEnum() {
        return this.fileType;
    }
}
