package org.irisacsee.trajlab.store;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.irisacsee.trajlab.constant.DBConstant;
import org.irisacsee.trajlab.constant.SetConstant;
import org.irisacsee.trajlab.index.IndexType;
import org.irisacsee.trajlab.index.type.TimeLine;
import org.irisacsee.trajlab.meta.DataSetMeta;
import org.irisacsee.trajlab.meta.IndexMeta;
import org.irisacsee.trajlab.meta.SetMeta;
import org.irisacsee.trajlab.model.MinimumBoundingBox;
import org.irisacsee.trajlab.serializer.ObjectSerializer;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author xuqi
 * @date 2023/12/03
 */
@Slf4j
public class MetaTable {
    private final Table dataSetMetaTable;

    public MetaTable(Table dataSetMetaTable) {
        this.dataSetMetaTable = dataSetMetaTable;
    }

    public DataSetMeta getDataSetMeta(String dataSetName) throws IOException {
        if (!dataSetExists(dataSetName)) {
            String msg = String.format("Dataset meta of [%s] does not exist", dataSetName);
            log.error(msg);
            throw new IOException(msg);
        } else {
            Result result = dataSetMetaTable.get(new Get(dataSetName.getBytes()));
            return fromResult(result);
        }
    }

    public boolean dataSetExists(String datasetName) throws IOException {
        return dataSetMetaTable.exists(new Get(datasetName.getBytes()));
    }

    public void putDataSet(DataSetMeta dataSetMeta) throws IOException {
        dataSetMetaTable.put(getPut(dataSetMeta));
    }

    public void deleteDataSetMeta(String datasetName) throws IOException {
        dataSetMetaTable.delete(new Delete(datasetName.getBytes()));
        log.info(
                String.format("Meta data of dataset [%s] has been removed from meta table", datasetName));
    }

    public List<DataSetMeta> listDataSetMeta() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = dataSetMetaTable.getScanner(scan);
        ArrayList<DataSetMeta> dataSetMetas = new ArrayList<>();
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            DataSetMeta dataSetMeta = fromResult(result);
            dataSetMetas.add(dataSetMeta);
        }
        return dataSetMetas;
    }

    /**
     * Convert DataSetMeta object into HBase Put object for dataset creation.
     */
    private Put getPut(DataSetMeta dataSetMeta) throws IOException {
        String dataSetName = dataSetMeta.getDataSetName();
        Map<IndexType, List<IndexMeta>> indexMetaMap = dataSetMeta.getAvailableIndexes();
        // row key - 数据集名称
        Put p = new Put(Bytes.toBytes(dataSetName));
        List<IndexMeta> indexMetaList = new ArrayList<>();
        for (IndexType type : indexMetaMap.keySet()) {
            indexMetaList.addAll(indexMetaMap.get(type));
        }
        // meta:index_meta
        p.addColumn(
                Bytes.toBytes(DBConstant.META_TABLE_COLUMN_FAMILY),
                Bytes.toBytes(DBConstant.META_TABLE_INDEX_META_QUALIFIER),
                ObjectSerializer.serializeList(indexMetaList, IndexMeta.class));
        // meta:main_table_meta
        p.addColumn(
                Bytes.toBytes(DBConstant.META_TABLE_COLUMN_FAMILY),
                Bytes.toBytes(DBConstant.META_TABLE_CORE_INDEX_META_QUALIFIER),
                ObjectSerializer.serializeObject(dataSetMeta.getMainIndexMeta()));
        // meta:desc
        p.addColumn(
                Bytes.toBytes(DBConstant.META_TABLE_COLUMN_FAMILY),
                Bytes.toBytes(DBConstant.META_TABLE_DESC_QUALIFIER),
                Bytes.toBytes(dataSetMeta.getDesc()));
        if (dataSetMeta.getSetMeta() != null) {
            SetMeta setMeta = dataSetMeta.getSetMeta();
            // meta:start_time
            p.addColumn(
                    Bytes.toBytes(DBConstant.META_TABLE_COLUMN_FAMILY),
                    Bytes.toBytes(SetConstant.START_TIME),
                    ObjectSerializer.serializeObject(setMeta.getStartTime()));
            // meta:srid
            p.addColumn(
                    Bytes.toBytes(DBConstant.META_TABLE_COLUMN_FAMILY),
                    Bytes.toBytes(SetConstant.SRID),
                    ObjectSerializer.serializeObject(setMeta.getSrid()));
            // meta:box
            p.addColumn(
                    Bytes.toBytes(DBConstant.META_TABLE_COLUMN_FAMILY),
                    SetConstant.DATA_MBR,
                    ObjectSerializer.serializeObject(setMeta.getBbox()));
            // meta:data_start_time
            p.addColumn(
                    Bytes.toBytes(DBConstant.META_TABLE_COLUMN_FAMILY),
                    SetConstant.DATA_START_TIME,
                    ObjectSerializer.serializeObject(setMeta.getTimeLine().getTimeStart()));
            // meta:data_end_time
            p.addColumn(
                    Bytes.toBytes(DBConstant.META_TABLE_COLUMN_FAMILY),
                    SetConstant.DATA_END_TIME,
                    ObjectSerializer.serializeObject(setMeta.getTimeLine().getTimeEnd()));
            // meta:data_count
            p.addColumn(
                    Bytes.toBytes(DBConstant.META_TABLE_COLUMN_FAMILY),
                    SetConstant.DATA_COUNT,
                    ObjectSerializer.serializeObject(setMeta.getDataCount()));
        }
        return p;
    }

    /**
     * Convert HBase Result object into DataSetMeta object.
     */
    private DataSetMeta fromResult(Result result) throws IOException {
        byte[] CF = Bytes.toBytes(DBConstant.META_TABLE_COLUMN_FAMILY);
        byte[] INDEX_METAS_CQ = Bytes.toBytes(DBConstant.META_TABLE_INDEX_META_QUALIFIER);
        byte[] MAIN_TABLE_CQ = Bytes.toBytes(DBConstant.META_TABLE_CORE_INDEX_META_QUALIFIER);
        String dataSetName = new String(result.getRow());
        Cell cell1 = result.getColumnLatestCell(CF, INDEX_METAS_CQ);
        Cell cell2 = result.getColumnLatestCell(CF, MAIN_TABLE_CQ);
        SetMeta setMetaFromResult = createSetMetaFromResult(result);
        List<IndexMeta> indexMetaList = ObjectSerializer.deserializeList(
                CellUtil.cloneValue(cell1), IndexMeta.class);
        IndexMeta mainTableMeta = (IndexMeta) ObjectSerializer.deserializeObject(
                CellUtil.cloneValue(cell2), IndexMeta.class);
        return new DataSetMeta(dataSetName, indexMetaList, mainTableMeta, setMetaFromResult);
    }

    private SetMeta createSetMetaFromResult(Result result) {
        byte[] cf = Bytes.toBytes(DBConstant.META_TABLE_COLUMN_FAMILY);
        byte[] startTimeCq = Bytes.toBytes(SetConstant.START_TIME);
        byte[] sridBytes = Bytes.toBytes(SetConstant.SRID);
        Cell cellStart = result.getColumnLatestCell(cf, startTimeCq);
        Cell cellSrid = result.getColumnLatestCell(cf, sridBytes);
        Cell cellSt = result.getColumnLatestCell(cf, SetConstant.DATA_START_TIME);
        Cell cellEt = result.getColumnLatestCell(cf, SetConstant.DATA_END_TIME);
        Cell cellCount = result.getColumnLatestCell(cf, SetConstant.DATA_COUNT);
        Cell cellMbr = result.getColumnLatestCell(cf, SetConstant.DATA_MBR);
        ZonedDateTime startTime = (ZonedDateTime) ObjectSerializer.deserializeObject(
                CellUtil.cloneValue(cellStart), ZonedDateTime.class);
        int srid = (int) ObjectSerializer.deserializeObject(CellUtil.cloneValue(cellSrid), Integer.class);
        ZonedDateTime dataStartTime = (ZonedDateTime) ObjectSerializer.deserializeObject(
                CellUtil.cloneValue(cellSt), ZonedDateTime.class);
        ZonedDateTime dataEndTime = (ZonedDateTime) ObjectSerializer.deserializeObject(
                CellUtil.cloneValue(cellEt), ZonedDateTime.class);
        int count = (int) ObjectSerializer.deserializeObject(CellUtil.cloneValue(cellCount), Integer.class);
        MinimumBoundingBox box = (MinimumBoundingBox) ObjectSerializer.deserializeObject(
                CellUtil.cloneValue(cellMbr), MinimumBoundingBox.class);
        TimeLine timeLine = new TimeLine(dataStartTime, dataEndTime);
        return new SetMeta(startTime, srid, box, timeLine, count);
    }

    public void close() {
        try {
            dataSetMetaTable.close();
        } catch (IOException e) {
            log.error("Failed to close meta table instance, exception log: {}", e.getMessage());
        }
    }
}
