package org.irisacsee.trajlab.store.mapper;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.irisacsee.trajlab.constant.DBConstant;
import org.irisacsee.trajlab.model.Trajectory;
import org.irisacsee.trajlab.model.TrajectoryMapper;
import org.irisacsee.trajlab.store.IndexTable;

import java.io.IOException;


/**
 * 主索引到辅助索引的mapper
 *
 * @author xuqi
 * @since 2024/11/22
 */
public class MainToSecondaryMapper extends TableMapper<ImmutableBytesWritable, Put> {

    private static IndexTable secondaryTable;

    @Override
    protected void setup(
            Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        secondaryTable = new IndexTable(context.getConfiguration().get(DBConstant.BULKLOAD_TARGET_INDEX_NAME));
    }

    @SuppressWarnings("rawtypes")
    public static void initJob(String table, Scan scan, Class<? extends TableMapper> mapper, Job job)
            throws IOException {
        TableMapReduceUtil.initTableMapperJob(
                table, scan, mapper, ImmutableBytesWritable.class, Result.class, job);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result coreIndexRow, Context context)
            throws IOException, InterruptedException {
        byte[] coreIndexRowKey = key.get();
        Trajectory t = TrajectoryMapper.getTrajectoryFromResult(coreIndexRow);
        Put p = TrajectoryMapper.mapTrajectoryToSecondrayIndexPut(
                t, secondaryTable.getIndexMeta().getIndexStrategy(), coreIndexRowKey);
        context.write(new ImmutableBytesWritable(p.getRow()), p);
    }
}