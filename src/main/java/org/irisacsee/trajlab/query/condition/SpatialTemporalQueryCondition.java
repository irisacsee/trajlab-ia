package org.irisacsee.trajlab.query.condition;

import org.irisacsee.trajlab.query.QueryType;

/**
 * 时空查询条件
 *
 * @author irisacsee
 * @since 2024/11/21
 */
public class SpatialTemporalQueryCondition extends AbstractQueryCondition {
    private final SpatialQueryCondition sqc;
    private final TemporalQueryCondition tqc;

    public SpatialTemporalQueryCondition(
            SpatialQueryCondition spatialQueryCondition,
            TemporalQueryCondition temporalQueryCondition) {
        sqc = spatialQueryCondition;
        tqc = temporalQueryCondition;
    }

    public SpatialQueryCondition getSpatialQueryCondition() {
        return sqc;
    }

    public TemporalQueryCondition getTemporalQueryCondition() {
        return tqc;
    }

    @Override
    public String getConditionInfo() {
        return "SpatialTemporalQueryCondition{" +
                "spatialQueryCondition=" + sqc +
                ", temporalQueryCondition=" + tqc +
                '}';
    }

    @Override
    public QueryType getInputType() {
        return QueryType.ST;
    }
}
