package org.irisacsee.trajlab;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.ARRAY;
import org.irisacsee.trajlab.util.SparkUtil;
import org.locationtech.jts.geom.Coordinate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 主类
 *
 * @author irisacsee
 * @since 2022/07/22
 */
public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
    }
}
