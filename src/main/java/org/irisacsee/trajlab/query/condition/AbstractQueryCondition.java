package org.irisacsee.trajlab.query.condition;

import org.irisacsee.trajlab.query.QueryType;

import java.io.Serializable;

/**
 * 查询条件抽象类
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public abstract class AbstractQueryCondition implements Serializable {
    public abstract String getConditionInfo();
    public abstract QueryType getInputType();
}
