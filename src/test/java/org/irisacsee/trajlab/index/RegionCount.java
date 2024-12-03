package org.irisacsee.trajlab.index;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.irisacsee.trajlab.store.HBaseConnector;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class RegionCount {
    public static void main(String[] args) throws IOException {
        Connection connection = HBaseConnector.getConnection();
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("T100ST-XZ2T-default");

        try (RegionLocator regionLocator = connection.getRegionLocator(tableName)) {
            List<HRegionLocation> regions = regionLocator.getAllRegionLocations();
            for (int i = 0; i < regions.size(); ++i) {
                HRegionLocation region = regions.get(i);
                HRegionInfo regionInfo = region.getRegionInfo();
                byte[] startKey = regionInfo.getStartKey();
                byte[] endKey = regionInfo.getEndKey();
                try (Table table = connection.getTable(tableName)) {
                    Scan scan = new Scan();
                    scan.withStartRow(startKey);
                    scan.withStopRow(endKey);

                    ResultScanner scanner = table.getScanner(scan);
                    int keyCount = 0;
                    for (Result result : scanner) {
                        ++keyCount;
                    }

                    System.out.println("Region: " + i + ", Key Count: " + keyCount);
                }
            }
        }

        admin.close();
        connection.close();
    }
}
