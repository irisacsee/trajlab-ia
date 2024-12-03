package org.irisacsee.trajlab.query.condition;

import lombok.Getter;
import org.irisacsee.trajlab.index.type.TimeLine;
import org.irisacsee.trajlab.query.QueryType;

import java.util.Collections;
import java.util.List;

/**
 * 时间查询条件
 *
 * @author irisacsee
 * @since 2024/11/21
 */
@Getter
public class TemporalQueryCondition extends AbstractQueryCondition {
    private final List<TimeLine> queryWindows;
    private final TemporalQueryType queryType;

    public TemporalQueryCondition(TimeLine queryWindow,
                                  TemporalQueryType queryType) {
        this.queryWindows = Collections.singletonList(queryWindow);
        this.queryType = queryType;
    }

    public TemporalQueryCondition(List<TimeLine> queryWindows,
                                  TemporalQueryType queryType) {
        this.queryWindows = queryWindows;
        this.queryType = queryType;
    }

    public boolean validate(TimeLine timeLine) {
        for (TimeLine queryWindow : queryWindows) {
            if (queryType == TemporalQueryType.CONTAIN ?
                    queryWindow.contain(timeLine) : queryWindow.intersect(timeLine)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getConditionInfo() {
        return "TemporalQueryCondition{" +
                "queryWindows=" + queryWindows +
                ", temporalQueryType=" + queryType +
                '}';
    }

    @Override
    public QueryType getInputType() {
        return QueryType.TEMPORAL;
    }

    public enum TemporalQueryType {
        /**
         * Query all data that may INTERSECT with query window.
         */
        CONTAIN,
        /**
         * Query all data that is totally contained in query window.
         */
        INTERSECT
    }
}
