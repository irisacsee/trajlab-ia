package org.irisacsee.trajlab.query;

import java.io.Serializable;

/**
 * 查询类型
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public enum QueryType implements Serializable {
    SPATIAL("spatial"),
    TEMPORAL("temporal"),
    ID("id"),
    ST("st"),
    ID_T("id-temporal"),
    DATASET("dataset"),
    KNN("knn"),
    SIMILAR("similar"),
    BUFFER("buffer"),
    ACCOMPANY("Accompany");
    private String queryType;

    QueryType(String queryType) {
        this.queryType = queryType;
    }

    @Override
    public String toString() {
        return "QueryType{" +
                "queryType='" + queryType + '\'' +
                '}';
    }
}
