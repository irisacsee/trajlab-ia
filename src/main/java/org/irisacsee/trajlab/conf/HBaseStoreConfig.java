package org.irisacsee.trajlab.conf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import org.irisacsee.trajlab.conf.enums.StoreSchema;
import org.irisacsee.trajlab.index.IndexType;
import org.irisacsee.trajlab.index.impl.JSTIndexStrategy;
import org.irisacsee.trajlab.index.impl.TXZ2IndexStrategy;
import org.irisacsee.trajlab.index.impl.XZ2IndexStrategy;
import org.irisacsee.trajlab.index.impl.XZ2TIndexStrategy;
import org.irisacsee.trajlab.index.impl.XZTIndexStrategy;
import org.irisacsee.trajlab.meta.DataSetMeta;
import org.irisacsee.trajlab.meta.IndexMeta;
import org.irisacsee.trajlab.conf.enums.FileSplitter;
import scala.NotImplementedError;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * HBase存储配置
 *
 * @author irisacsee
 * @since 2024/11/22
 */
@Getter
public class HBaseStoreConfig implements IStoreConfig {
    private final String location;
    private final String dataSetName;
    private final IndexType mainIndex;
    @Setter
    private String otherIndex;
    private final StoreSchema schema;
    private final List<IndexMeta> indexList;
    private final DataSetMeta dataSetMeta;

    @JsonCreator
    public HBaseStoreConfig(
            @JsonProperty("location") String location,
            @JsonProperty("dataSetName") String dataSetName,
            @JsonProperty("schema") StoreSchema schema,
            @JsonProperty("mainIndex") IndexType mainIndex,
            @JsonProperty("otherIndex") @JsonInclude(JsonInclude.Include.NON_NULL) String otherIndex) {
        this.location = location;
        this.dataSetName = dataSetName;
        this.schema = schema;
        this.mainIndex = mainIndex;
        this.otherIndex = otherIndex;
        this.indexList = createIndexList();
        this.dataSetMeta = new DataSetMeta(this.dataSetName, this.indexList);
    }

    private List<IndexMeta> createIndexList() {
        List<IndexMeta> indexMetaList = new LinkedList<>();
        IndexMeta mainIndexMeta = createIndexMeta(mainIndex, true);
        indexMetaList.add(mainIndexMeta);
        if (otherIndex != null) {
            List<IndexMeta> otherIndexMeta = createOtherIndex(otherIndex, FileSplitter.CSV);
            indexMetaList.addAll(otherIndexMeta);
        }
        checkIndexMeta(indexMetaList, mainIndexMeta);
        return indexMetaList;
    }
    private void checkIndexMeta(List<IndexMeta> indexMetaList, IndexMeta mainIndexMeta){
        // 检查重复
        HashSet<IndexMeta> hashSet = new HashSet<>(indexMetaList);
        if (hashSet.size() != indexMetaList.size()) {
            throw new IllegalArgumentException("found duplicate index meta in the list.");
        }
    }

    private IndexMeta createIndexMeta(IndexType indexType, Boolean isMainIndex) {
        switch (indexType) {
            case XZ2:
                return new IndexMeta(isMainIndex, new XZ2IndexStrategy(), dataSetName, "default");
            case XZT:
                return new IndexMeta(isMainIndex, new XZTIndexStrategy(), dataSetName, "default");
            case XZ2T:
                return new IndexMeta(isMainIndex, new XZ2TIndexStrategy(), dataSetName, "default");
            case TXZ2:
                return new IndexMeta(isMainIndex, new TXZ2IndexStrategy(), dataSetName, "default");
            case JST:
                return new IndexMeta(isMainIndex, new JSTIndexStrategy(), dataSetName, "default");
            default:
                throw new NotImplementedError();
        }
    }

    private List<IndexMeta> createOtherIndex(String otherIndex, FileSplitter splitType) {
        String[] indexValue = otherIndex.split(splitType.getDelimiter());
        ArrayList<IndexMeta> indexMetaList = new ArrayList<>();
        for (String index : indexValue) {
            IndexType indexType = IndexType.valueOf(index);
            IndexMeta indexMeta = createIndexMeta(indexType, false);
            indexMetaList.add(indexMeta);
        }
        return indexMetaList;
    }

    @Override
    public String toString() {
        return "HBaseStoreConfig{" +
                "mainIndex=" + mainIndex +
                ", otherIndex='" + otherIndex + '\'' +
                ", dataSetMeta=" + dataSetMeta +
                '}';
    }
}
