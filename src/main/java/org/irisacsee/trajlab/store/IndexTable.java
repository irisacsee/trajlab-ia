package org.irisacsee.trajlab.store;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.irisacsee.trajlab.index.type.RowKeyRange;
import org.irisacsee.trajlab.meta.DataSetMeta;
import org.irisacsee.trajlab.meta.IndexMeta;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.model.TrajectoryMapper;
import org.irisacsee.trajlab.query.condition.AbstractQueryCondition;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 索引表
 * 
 * @author irisacsee
 * @since 2024/11/22
 */
@Getter
public class IndexTable<QC extends AbstractQueryCondition> implements Serializable {
    @Setter
    private Table table;
    private final IndexMeta<QC> indexMeta;

    public IndexTable(IndexMeta<QC> indexMeta) throws IOException {
        this.indexMeta = indexMeta;
        this.table = HBaseConnector
                .getConnection()
                .getTable(TableName.valueOf(indexMeta.getIndexTableName()));
    }

    public IndexTable(String tableName) throws IOException {
        indexMeta = DataSetMeta.getIndexMetaByName(HBaseConnector
                .getDataSetMeta(extractDataSetName(tableName))
                .getIndexMetaList(), tableName);
        table = HBaseConnector
                .getConnection()
                .getTable(TableName.valueOf(tableName));;
    }

    /**
     * 将轨迹put至主表所使用的方法。若该数据集有多份索引，则put至非主表的各索引表的工作由主表上的Observer协处理器完成。
     */
    public void putForMainTable(Trajectory trajectory) throws IOException {
        table.put(TrajectoryMapper.mapTrajectoryToMainIndexPut(trajectory, indexMeta.getIndexStrategy()));
    }

    public void putForSecondaryTable(Trajectory trajectory, byte[] ptr) throws IOException {
        Put put = TrajectoryMapper.mapTrajectoryToSecondrayIndexPut(trajectory, indexMeta.getIndexStrategy(), ptr);
        table.put(put);
    }

    public void put(Trajectory trajectory, @Nullable byte[] ptr) throws IOException {
        if (indexMeta.isMainIndex()) {
            putForMainTable(trajectory);
        } else {
            putForSecondaryTable(trajectory, ptr);
        }
    }

    public Result get(Get get) throws IOException {
        return table.get(get);
    }

    public void delete(Delete delete) throws IOException {
        table.delete(delete);
    }

    public ResultScanner getScanner(Scan scan) throws IOException {
        return table.getScanner(scan);
    }

    public List<Result> scan(RowKeyRange rowKeyRange) throws IOException {
        Scan scan = new Scan();
        scan.withStartRow(rowKeyRange.getStartKey());
        scan.withStopRow(rowKeyRange.getEndKey());
        List<Result> results = new ArrayList<>();
        for (Result r : table.getScanner(scan)) {
            results.add(r);
        }
        return results;
    }

    public void close() throws IOException {
        table.close();
    }

    // 表名结构: DataSetName-IndexType-Suffix
    public static String extractDataSetName(IndexTable indexTable) {
        String tableName = indexTable.getTable().getName().getNameAsString();
        return extractDataSetName(tableName);
    }

    // 表名结构: DataSetName-IndexType-Suffix
    public static String extractDataSetName(String tableName) {
        String[] strs = tableName.split("-");
        String indexTypeStr = strs[strs.length - 2];
        String suffix = strs[strs.length - 1];
        return tableName.substring(0, tableName.length() - indexTypeStr.length() - suffix.length() - 2);
    }
}
