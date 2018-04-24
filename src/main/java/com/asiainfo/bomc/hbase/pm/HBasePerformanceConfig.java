package com.asiainfo.bomc.hbase.pm;

import com.ai.toptea.sysm.pm.hbase.DateUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by suzengshuai on 2018/4/24.
 */
public class HBasePerformanceConfig {
    public static final byte[] COLUMN_FAMILY_VALUE = Bytes.toBytes("v");
    public static final String TABLE_NAME = "sysm:pm_perf_";
    public static final String STATISTICSTATE_TABLENAME = "sysm:pm_statistic";
    private int saveMode = 1;
    private String zookeeper;
    private int port;
    private int posMoId = 32;
    private int posBeginTime = 40;
    private int posEndTime = 48;

    public static final int MODE_MONTH = 0;
    public static final int MODE_YEAR = 1;
    public static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat SDF_MONTH = new SimpleDateFormat("yyyy-MM");
    public static final SimpleDateFormat SDF_YEAR = new SimpleDateFormat("yyyy");
    public static final int size = 1;
    private static String[] formats_year;
    private static long[] begins_year;
    private static String[] formats_month;
    private static long[] begins_month;

    public HBasePerformanceConfig() {
    }

    public byte[] generateRowKey(String moId, long beginTime, long endTime, Object key) {
        return this.generateRowKey(moId, beginTime, endTime, String.valueOf(key), false, false);
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


    public long getBeginTimeFromRowKey(byte[] rowkey) {
        return ~Bytes.toLong(rowkey, this.posMoId, 8);
    }

    public long getEndTimeFromRowKey(byte[] rowkey) {
        return ~Bytes.toLong(rowkey, this.posBeginTime, 8);
    }

    public String getPrimaryKey(byte[] rowkey) {
        return Bytes.toString(rowkey, this.posEndTime);
    }

    public String getHTableName(long time) {
        return "sysm:pm_perf_" + DateUtil.getFormat(time, this.saveMode);
    }

    public int getSaveMode() {
        return this.saveMode;
    }

    public void setSaveMode(int saveMode) {
        this.saveMode = saveMode;
    }

    public String getZookeeper() {
        return this.zookeeper;
    }

    public void setZookeeper(String zookeeper) {
        this.zookeeper = zookeeper;
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPosMoId() {
        return this.posMoId;
    }

    public void setPosMoId(int posMoId) {
        this.posMoId = posMoId;
        this.posBeginTime = this.posMoId + 8;
        this.posEndTime = this.posBeginTime + 8;
    }

    public static String getFormat(long time, int mode) {
        return mode == 0?_getFormat(time, formats_month, begins_month, SDF_MONTH):(mode == 1?_getFormat(time, formats_year, begins_year, SDF_YEAR):"");
    }

    private static String _getFormat(long time, String[] formats, long[] begins, SimpleDateFormat sdf) {
        int index = -1;

        for(int i = begins.length - 1; i >= 0; --i) {
            if(time >= begins[i]) {
                index = i;
                break;
            }
        }

        if(index == begins.length - 1) {
            index = -1;
        }

        if(index != -1) {
            return formats[index];
        } else {
            System.out.println("out of bounds");
            Date d = new Date(time);
            return sdf.format(d);
        }
    }
}
