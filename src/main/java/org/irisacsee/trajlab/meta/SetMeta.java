package org.irisacsee.trajlab.meta;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.irisacsee.trajlab.constant.SetConstant;
import org.irisacsee.trajlab.index.type.TimeLine;
import org.irisacsee.trajlab.model.MinimumBoundingBox;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.util.DateUtil;
import scala.Tuple4;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据集具体元信息
 *
 * @author irisacsee
 * @since 2024/11/21
 */
@Getter
public class SetMeta implements Serializable {
    private ZonedDateTime startTime = DateUtil.parseDate(SetConstant.START_TIME);
    private int srid = SetConstant.SRID;
    private MinimumBoundingBox bbox;
    private TimeLine timeLine;
    private int dataCount;

    public SetMeta(ZonedDateTime startTime, int srid, MinimumBoundingBox bbox, TimeLine timeLine, int dataCount) {
        this.startTime = startTime;
        this.srid = srid;
        this.bbox = bbox;
        this.timeLine = timeLine;
        this.dataCount = dataCount;
    }

    public SetMeta(MinimumBoundingBox bbox, TimeLine timeLine, int dataCount) {
        this.bbox = bbox;
        this.timeLine = timeLine;
        this.dataCount = dataCount;
    }

    public SetMeta(JavaRDD<Trajectory> trajectoryRdd) {
        buildSetMetaFromRDD(trajectoryRdd);
    }

    /**
     * 从RDD构建数据集具体元信息
     *
     * @param trajectoryRdd 轨迹数据RDD，注意传参前轨迹已计算完特征
     */
    private void buildSetMetaFromRDD(JavaRDD<Trajectory> trajectoryRdd) {
        Tuple4<MinimumBoundingBox, ZonedDateTime, ZonedDateTime, Integer> meta = trajectoryRdd
                .mapPartitions(itr -> {
                    List<Tuple4<MinimumBoundingBox, ZonedDateTime, ZonedDateTime, Integer>> localBoxes =
                            new ArrayList<>();
                    int localCount = 0;
                    if (itr.hasNext()) {
                        Trajectory first = itr.next();
                        MinimumBoundingBox localBox = first.getTrajectoryFeatures().getMbr();
                        ZonedDateTime localStartTime = first.getTrajectoryFeatures().getStartTime();
                        ZonedDateTime localEndTime = first.getTrajectoryFeatures().getEndTime();
                        ++localCount;
                        while (itr.hasNext()) {
                            Trajectory t = itr.next();
                            localBox = localBox.union(t.getTrajectoryFeatures().getMbr());
                            localStartTime = localStartTime.compareTo(t.getTrajectoryFeatures().getStartTime()) <= 0
                                    ? localStartTime
                                    : t.getTrajectoryFeatures().getStartTime();
                            localEndTime = localEndTime.compareTo(t.getTrajectoryFeatures().getEndTime()) >= 0
                                    ? localEndTime
                                    : t.getTrajectoryFeatures().getEndTime();
                            ++localCount;
                        }
                        localBoxes.add(new Tuple4<>(localBox, localStartTime, localEndTime, localCount));
                    }
                    return localBoxes.iterator();
                })
                .reduce((box1, box2) -> new Tuple4<>(
                        box1._1().union(box2._1()),
                        box1._2().isBefore(box2._2()) ? box1._2() : box2._2(),
                        box1._3().isAfter(box2._3()) ? box1._3() : box2._3(),
                        box1._4() + box2._4()));
        bbox = meta._1();
        timeLine = new TimeLine(meta._2(), meta._3());
        dataCount = meta._4();
    }

    @Override
    public String toString() {
        return "SetMeta{"
                + "startTime="
                + startTime
                + ", srid="
                + srid
                + ", bbox="
                + bbox
                + ", timeLine="
                + timeLine
                + ", dataCount="
                + dataCount
                + '}';
    }
}
