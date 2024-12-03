package org.irisacsee.trajlab.conf.enums;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * 文件模式
 *
 * @author irisacsee
 * @since 2024/11/23
 */
public enum FileMode {
    MULTI_FILE("multi_file"),
    SINGLE_FILE("single_file"),
    MULTI_SINGLE_FILE("multi_single_file");

    private final String mode;

    FileMode(String mode) {
        this.mode = mode;
    }

    @JsonValue
    public String getMode() {
        return this.mode;
    }
}