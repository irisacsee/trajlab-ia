package org.irisacsee.trajlab.conf.enums;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 文件分割符枚举
 *
 * @author irisacsee
 * @since 2024/11/22
 */
public enum FileSplitter implements Serializable {
    /**
     * The csv.
     */
    CSV(","),

    /**
     * The tsv.
     */
    TSV("\t"),

    /**
     * The geojson.
     */
    GEOJSON(""),

    /**
     * The wkt.
     */
    WKT("\t"),

    /**
     * The wkb.
     */
    WKB("\t"),

    COMMA(","),

    TAB("\t"),

    QUESTIONMARK("?"),

    SINGLEQUOTE("\'"),

    QUOTE("\""),

    UNDERSCORE("_"),

    DASH("-"),

    PERCENT("%"),

    TILDE("~"),

    PIPE("|"),

    SEMICOLON(";");

    /**
     * The splitter.
     */
    private final String splitter;

    // A lookup map for getting a FileDataSplitter from a delimiter, or its name
    @SuppressWarnings("checkstyle:ConstantName")
    private static final Map<String, FileSplitter> lookupMap =
            new HashMap<String, FileSplitter>();

    static {
        for (FileSplitter f : FileSplitter.values()) {
            lookupMap.put(f.getDelimiter(), f);
            lookupMap.put(f.name().toLowerCase(), f);
            lookupMap.put(f.name().toUpperCase(), f);
        }
    }

    /**
     * Instantiates a new file data splitter.
     *
     * @param splitter the splitter
     */
    FileSplitter(String splitter) {
        this.splitter = splitter;
    }

    /**
     * Gets the file data splitter.
     *
     * @param str the str
     * @return the file data splitter
     */
    public static FileSplitter getFileDataSplitter(String str) {
        FileSplitter f = lookupMap.get(str);
        if (f == null) {
            throw new IllegalArgumentException(
                    "[" + FileSplitter.class + "] Unsupported FileDataSplitter:" + str);
        }
        return f;
    }

    /**
     * Gets the delimiter.
     *
     * @return the delimiter
     */
    public String getDelimiter() {
        return this.splitter;
    }
}
