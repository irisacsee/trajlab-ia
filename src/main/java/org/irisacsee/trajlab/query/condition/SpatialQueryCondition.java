package org.irisacsee.trajlab.query.condition;

import lombok.Data;
import org.irisacsee.trajlab.query.QueryType;
import org.irisacsee.trajlab.query.condition.AbstractQueryCondition;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;

/**
 * 空间查询条件
 *
 * @author irisacsee
 * @since 2024/11/20
 */
@Data
public class SpatialQueryCondition extends AbstractQueryCondition {
    private Envelope queryWindow;
    private Geometry geometryWindow;
    private SpatialQueryType queryType;

    public SpatialQueryCondition(SpatialQueryType queryType) {
        this.queryType = queryType;
    }

    public SpatialQueryCondition(Geometry geometryWindow, SpatialQueryType queryType) {
        this.geometryWindow = geometryWindow;
        this.queryWindow = geometryWindow.getEnvelopeInternal();
        this.queryType = queryType;
    }

    public void updateWindow(Geometry newWindow) {
        geometryWindow = newWindow;
        queryWindow = newWindow.getEnvelopeInternal();
    }

    public String getQueryWindowWKT() {
        WKTWriter writer = new WKTWriter();
        return writer.write(geometryWindow);
    }

    @Override
    public String getConditionInfo() {
        return "SpatialQueryCondition{" +
                "queryWindow=" + queryWindow +
                ", queryType=" + queryType +
                '}';
    }

    @Override
    public QueryType getInputType() {
        return QueryType.SPATIAL;
    }

    /**
     * 将查询窗口用于什么样的查询: 两类: 严格包含查询\相交包含查询
     *
     * @author Haocheng Wang
     * Created on 2022/9/27
     */
    public enum SpatialQueryType {
        /**
         * Query all data that may INTERSECT with query window.
         */
        CONTAIN,
        /**
         * Query all data that is totally contained in query window.
         */
        INTERSECT;
    }
}
