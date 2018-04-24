package com.asiainfo.bomc.hbase.pm;

import com.ai.toptea.sysm.pm.PerformanceRow;
import com.ai.toptea.sysm.pm.hbase.HBasePerformanceConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by suzengshuai on 2018/4/24.
 */
public class PerformanceManager {
    public static final byte[] COLUMN_FAMILY_VALUE = Bytes.toBytes("v");
    public static final String TABLE_NAME = "sysm:pm_perf_";
    public static final String STATISTICSTATE_TABLENAME = "sysm:pm_statistic";
    private int saveMode = 1;
    private String zookeeper;
    private int port;
    private int posMoId = 32;
    private int posBeginTime = 40;
    private int posEndTime = 48;



    private Connection connection;
    public void init() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.17.1.216,10.17.1.218,10.17.1.217");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            this.connection = ConnectionFactory.createConnection(conf);
        } catch (Exception var3) {

        }
    }


    public void getPer(){

            Table table = null;

            try {
                long time = System.currentTimeMillis();
                table = this.connection.getTable(TableName.valueOf("sysmgr:pm_perf_2018"));
//                table = this.connection.getTable(TableName.valueOf(TableName.valueOf(tableName));

                Scan scan = new Scan();
                scan.addFamily(HBasePerformanceConfig.COLUMN_FAMILY_VALUE);

                for(int i = 0; i < perfNames.length; ++i) {
                    scan.addColumn(HBasePerformanceConfig.COLUMN_FAMILY_VALUE, perfNameBytes[i]);
                }

                if(desc) {
                    scan.setStartRow(this.storeConfig.generateRowKey(moId, endTime, -1L, primaryKey, false, true));
                    scan.setStopRow(this.storeConfig.generateRowKey(moId, beginTime, -1L, primaryKey, true, true));
                } else {
                    scan.setStartRow(this.storeConfig.generateRowKey(moId, beginTime, -1L, primaryKey, false, true));
                    scan.setStopRow(this.storeConfig.generateRowKey(moId, endTime, -1L, primaryKey, true, true));
                }

                scan.setCaching(this.scannerCaching);
                if(length != -1) {
                    Filter filter = new PageFilter((long)length);
                    scan.setFilter(filter);
                }

                ResultScanner scanner = table.getScanner(scan);
                byte[] rowkey = null;
                PerformanceRow pr = null;

                for(Iterator var22 = scanner.iterator(); var22.hasNext(); list.add(pr)) {
                    Result rs = (Result)var22.next();
                    byte[] rowkey = rs.getRow();
                    pr = new PerformanceRow(this.storeConfig.getBeginTimeFromRowKey(rowkey), this.storeConfig.getEndTimeFromRowKey(rowkey), perfNames.length);

                    for(int i = 0; i < perfNames.length; ++i) {
                        if(rs.containsColumn(HBasePerformanceConfig.COLUMN_FAMILY_VALUE, perfNameBytes[i])) {
                            pr.setValue(i, Bytes.toString(rs.getValue(HBasePerformanceConfig.COLUMN_FAMILY_VALUE, perfNameBytes[i])));
                        }
                    }

                    if(list == null) {
                        list = new ArrayList();
                    }
                }

                scanner.close();
            } catch (Exception var33) {

            } finally {
                try {
                    table.close();
                } catch (IOException var32) {
                }
            }


    }


    public byte[] generateRowKey(String moId, long beginTime, long endTime, String key, boolean stopRow, boolean query) {
        int size = this.posMoId;
        size += 8;
        size += 8;
        byte[] b_key = null;
        if(key != null && !"".equals(key)) {
            b_key = Bytes.toBytes(key);
            size += b_key.length;
        }

        byte[] rowkey = new byte[size];
        byte[] b_moId = Bytes.toBytes(moId);
        System.arraycopy(b_moId, 0, rowkey, 0, b_moId.length);
        if(stopRow && beginTime == -1L) {
            ++rowkey[b_moId.length - 1];
        }

        byte[] b_endTime;
        if(beginTime != -1L) {
            if(query) {
                --beginTime;
            }

            b_endTime = Bytes.toBytes(~beginTime);
            System.arraycopy(b_endTime, 0, rowkey, this.posMoId, 8);
        }

        if(endTime != -1L) {
            b_endTime = Bytes.toBytes(~endTime);
            System.arraycopy(b_endTime, 0, rowkey, this.posBeginTime, 8);
        }

        if(key != null && !"".equals(key)) {
            System.arraycopy(b_key, 0, rowkey, this.posEndTime, b_key.length);
        }

        return rowkey;
    }
}
