package org.irisacsee.trajlab.query.condition;

import org.irisacsee.trajlab.model.BasePoint;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.query.QueryType;
import org.irisacsee.trajlab.serializer.ObjectSerializer;

import java.io.IOException;
import java.io.Serializable;

/**
 * KNN查询条件
 *
 * @author irisacsee
 * @since 2024/11/21
 */
public class KNNQueryCondition extends AbstractQueryCondition {
    private final int k;
    private final BasePoint centralPoint;
    private final Trajectory centralTrajectory;
    private final TemporalQueryCondition temporalQueryCondition;
    private final KNNQueryType knnQueryType;

    public KNNQueryCondition(int k, Trajectory centralTrajectory, TemporalQueryCondition temporalQueryCondition) {
        this.k = k;
        this.centralPoint = null;
        this.centralTrajectory = centralTrajectory;
        this.temporalQueryCondition = temporalQueryCondition;
        this.knnQueryType = KNNQueryType.TRAJECTORY;
    }

    public KNNQueryCondition(int k, BasePoint centralPoint, TemporalQueryCondition temporalQueryCondition) {
        this.k = k;
        this.centralPoint = centralPoint;
        this.centralTrajectory = null;
        this.temporalQueryCondition = temporalQueryCondition;
        this.knnQueryType = KNNQueryType.POINT;
    }

    public KNNQueryCondition(int k, Trajectory centralTrajectory) {
        this.k = k;
        this.centralPoint = null;
        this.centralTrajectory = centralTrajectory;
        this.temporalQueryCondition = null;
        this.knnQueryType = KNNQueryType.TRAJECTORY;
    }

    public KNNQueryCondition(int k, BasePoint centralPoint) {
        this.k = k;
        this.centralPoint = centralPoint;
        this.centralTrajectory = null;
        this.temporalQueryCondition = null;
        this.knnQueryType = KNNQueryType.POINT;
    }

    public int getK() {
        return k;
    }

    public BasePoint getCentralPoint() {
        return centralPoint;
    }

    public Trajectory getCentralTrajectory() {
        return centralTrajectory;
    }

    public TemporalQueryCondition getTemporalQueryCondition() {
        return temporalQueryCondition;
    }

    public KNNQueryType getKnnQueryType() {
        return knnQueryType;
    }

    public byte[] getPointBytes() throws IOException {
        return ObjectSerializer.serializeObject(centralPoint);
    }

    public byte[] getTrajectoryBytes() throws IOException {
        return ObjectSerializer.serializeObject(centralTrajectory);
    }

    @Override
    public String getConditionInfo() {
        return "KNNQueryCondition{" +
                "k=" + k +
                ", centralPoint=" + centralPoint +
                ", centralTrajectory=" + centralTrajectory +
                ", temporalQueryCondition=" + temporalQueryCondition +
                '}';
    }

    @Override
    public QueryType getInputType() {
        return QueryType.KNN;
    }

    public enum KNNQueryType implements Serializable {
        POINT("point"),
        TRAJECTORY("trajectory");

        private final String queryType;

        KNNQueryType(String queryType) {
            this.queryType = queryType;
        }

        @Override
        public String toString() {
            return "QueryType{" +
                    "queryType='" + queryType + '\'' +
                    '}';
        }
    }
}
