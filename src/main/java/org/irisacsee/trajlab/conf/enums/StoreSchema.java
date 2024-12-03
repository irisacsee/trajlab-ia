package org.irisacsee.trajlab.conf.enums;

/**
 * 存储模式
 *
 * @author irisacsee
 * @since 2024/11/23
 */
public enum StoreSchema {
    POINT_BASED_TRAJECTORY("trajectory_point"),
    LIST_BASED_TRAJECTORY("trajectory_list"),
    STAY_POINT("stay_point"),
    POINT_BASED_TRAJECTORY_SLOWPUT("trajectory_point_slowput");

    private final String storeSchema;

    StoreSchema(String storeSchema) {
        this.storeSchema = storeSchema;
    }

    public final String getType() {
        return this.storeSchema;
    }

    public static class Constants {
        public Constants() {
        }
    }
}