package org.irisacsee.trajlab.query.condition;

import org.irisacsee.trajlab.model.BasePoint;
import org.irisacsee.trajlab.util.GeoUtil;
import org.locationtech.jts.geom.Geometry;

/**
 * 查询条件转换器
 *
 * @author irisacsee
 * @since 2024/11/22
 */
public class QueryConditionMapper {
    public static SpatialTemporalQueryCondition mapKNNToSpatialTemporal(
            KNNQueryCondition knnQueryCondition, double curSearchDist) {
        BasePoint centralPoint = knnQueryCondition.getCentralPoint();
        Geometry buffer = centralPoint.buffer(GeoUtil.getDegreeFromKm(curSearchDist));
        SpatialQueryCondition sqc = new SpatialQueryCondition(buffer, SpatialQueryCondition.SpatialQueryType.INTERSECT);
        TemporalQueryCondition tqc = knnQueryCondition.getTemporalQueryCondition();
        return new SpatialTemporalQueryCondition(sqc, tqc);
    }
}
