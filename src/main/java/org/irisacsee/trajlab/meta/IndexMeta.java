package org.irisacsee.trajlab.meta;

import lombok.Getter;
import org.irisacsee.trajlab.index.IndexStrategy;
import org.irisacsee.trajlab.index.IndexType;
import org.irisacsee.trajlab.query.condition.AbstractQueryCondition;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * 索引元信息
 *
 * @author irisacsee
 * @since 2024/11/21
 */
public class IndexMeta<QC extends AbstractQueryCondition> implements Serializable {
    private final boolean isMainIndex;
    @Getter
    private final IndexStrategy<QC> indexStrategy;
    @Getter
    private final String dataSetName;
    @Getter
    private final String indexTableName;

    /**
     * 使用此构造方法创建的IndexMeta本身即为core index
     */
    public IndexMeta(boolean isMainIndex,
                     IndexStrategy<QC> indexStrategy,
                     String dataSetName,
                     String tableNameSuffix) {
        this.isMainIndex = isMainIndex;
        this.indexStrategy = indexStrategy;
        this.dataSetName = dataSetName;
        this.indexTableName = dataSetName + "-" + indexStrategy.getIndexType().name() + "-" + tableNameSuffix;
    }

    public IndexType getIndexType() {
        return indexStrategy.getIndexType();
    }

    public boolean isMainIndex() {
        return isMainIndex;
    }
    @Override
    public String toString() {
        return "IndexMeta{"
                + "isMainIndex=" + isMainIndex
                + ", indexStrategy=" + indexStrategy
                + ", dataSetName='" + dataSetName + '\''
                + ", indexTableName='" + indexTableName + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexMeta<QC> indexMeta = (IndexMeta<QC>) o;
        return isMainIndex == indexMeta.isMainIndex
                && Objects.equals(indexStrategy, indexMeta.indexStrategy)
                && Objects.equals(dataSetName, indexMeta.dataSetName)
                && Objects.equals(indexTableName, indexMeta.indexTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isMainIndex, indexStrategy, dataSetName, indexTableName);
    }

    /**
     * 从相同IndexType的多个IndexMeta找出最适于查询的IndexMeta.
     * 当前的判定依据为选取其中代表主索引的IndexMeta，如果全部是辅助索引，则取其中的第一个。
     * 未来可加入其他因素，如数据集是否已经清洗、索引的参数，或者结合查询条件选择最佳的索引。
     * @param indexMetaList 相同IndexType的多个IndexMeta
     * @return 最适于查询的IndexMeta
     */
    public static IndexMeta getBestIndexMeta(List<IndexMeta> indexMetaList) {
        for (IndexMeta indexMeta : indexMetaList) {
            if (indexMeta.isMainIndex()) {
                return indexMeta;
            }
        }
        return indexMetaList.get(0);
    }

    public byte[][] getSplits() {
        return indexStrategy.getSplits();
    }
}
