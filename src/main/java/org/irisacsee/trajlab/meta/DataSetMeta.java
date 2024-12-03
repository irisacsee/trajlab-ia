package org.irisacsee.trajlab.meta;

import lombok.Getter;
import lombok.Setter;
import org.irisacsee.trajlab.index.IndexType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 数据集元信息
 *
 * @author irisacsee
 * @since 2024/11/21
 */
@Getter
public class DataSetMeta implements Serializable {
    private final String dataSetName;
    private final List<IndexMeta> indexMetaList;
    private final IndexMeta mainIndexMeta;
    @Setter
    private SetMeta setMeta;
    private String desc = "";

    /**
     * 将默认List中的第一项为主索引.
     *
     * @param dataSetName
     * @param indexMetaList
     */
    public DataSetMeta(String dataSetName, List<IndexMeta> indexMetaList) {
        this(dataSetName, indexMetaList, getMainIndexMetaFromList(indexMetaList));
    }

    public DataSetMeta(String dataSetName, List<IndexMeta> indexMetaList, IndexMeta mainIndexMeta) {
        checkMainIndexMeta(indexMetaList, mainIndexMeta);
        this.dataSetName = dataSetName;
        this.indexMetaList = indexMetaList;
        this.mainIndexMeta = mainIndexMeta;
    }

    public DataSetMeta(
            String dataSetName, List<IndexMeta> indexMetaList, IndexMeta mainIndexMeta, SetMeta setMeta) {
        this.dataSetName = dataSetName;
        this.indexMetaList = indexMetaList;
        this.mainIndexMeta = mainIndexMeta;
        this.setMeta = setMeta;
    }

    public DataSetMeta(
            String dataSetName, List<IndexMeta> indexMetaList, IndexMeta mainIndexMeta, String desc) {
        this(dataSetName, indexMetaList, mainIndexMeta);
        this.desc = desc;
    }

    public DataSetMeta(
            String dataSetName,
            List<IndexMeta> indexMetaList,
            IndexMeta mainIndexMeta,
            SetMeta setMeta,
            String desc) {
        this(dataSetName, indexMetaList, mainIndexMeta, setMeta);
        this.desc = desc;
    }

    public Map<IndexType, List<IndexMeta>> getAvailableIndexes() {
        List<IndexMeta> indexMetaList = getIndexMetaList();
        Map<IndexType, List<IndexMeta>> map = new HashMap<>();
        for (IndexMeta indexMeta : indexMetaList) {
            List<IndexMeta> list = map.getOrDefault(indexMeta.getIndexType(), new ArrayList<>());
            list.add(indexMeta);
            map.put(indexMeta.getIndexType(), list);
        }
        return map;
    }

    @Override
    public String toString() {
        return "DataSetMeta{" +
                "dataSetName='" + dataSetName + '\'' +
                ", indexMetaList=" + indexMetaList +
                ", mainIndexMeta=" + mainIndexMeta +
                ", setMeta=" + setMeta +
                ", desc='" + desc + '\'' +
                '}';
    }

    private static IndexMeta getMainIndexMetaFromList(List<IndexMeta> indexMetaList) {
        for (IndexMeta im : indexMetaList) {
            if (im.isMainIndex()) {
                return im;
            }
        }
        return null;
    }

    private void checkMainIndexMeta(List<IndexMeta> indexMetaList, IndexMeta mainIndexMeta)
            throws IllegalArgumentException {
        // 检查重复
        Set<IndexMeta> set = new HashSet<>(indexMetaList);
        if (set.size() != indexMetaList.size()) {
            throw new IllegalArgumentException("found duplicate index meta in the list.");
        }
        // 确认mainIndex存在
        if (mainIndexMeta == null) {
            throw new IllegalArgumentException(String.format("Index meta didn't set core index."));
        }
    }

    public static IndexMeta getIndexMetaByName(List<IndexMeta> indexMetaList, String tableName) {
        for (IndexMeta im : indexMetaList) {
            if (im.getIndexTableName().equals(tableName)) {
                return im;
            }
        }
        return null;
    }
}
