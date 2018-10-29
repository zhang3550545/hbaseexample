package com.example;

import com.example.bean.HColumnBean;
import com.example.bean.HColumnDescriptorBean;
import com.example.bean.HTableDescriptorBean;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;


import java.io.IOException;
import java.util.*;


public class HbaseTest {
    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();

        HbaseManager manager = HbaseManager.getInstance();
        Connection connection = manager.getConnectNotNull();
        TableName tableName = TableName.valueOf("sit_my_account:user_asset");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        scan.setTimeRange(1538201289000L, 1538209389000L);
        ResultScanner scanner = table.getScanner(scan);
        int size = Iterables.size(scanner);
        System.out.println(size);
        System.out.println(System.currentTimeMillis() - startTime);

        // 释放资源
        manager.closeConnect();
    }

    private static void cacheTest() throws IOException {
        scanCacheTest(1, 1);
        scanCacheTest(200, 1);
        scanCacheTest(2000, 100);
        scanCacheTest(2, 100);
        scanCacheTest(2, 10);
        scanCacheTest(5, 100);
        scanCacheTest(5, 20);
        scanCacheTest(10, 10);
    }

    private static void scanCacheTest(int caching, int batch) throws IOException {
        HbaseManager manager = HbaseManager.getInstance();
        Logger logger = Logger.getLogger("org.apache.hadoop");
        int[] counts = {0, 0};
        Appender appender = new AppenderSkeleton() {
            @Override
            protected void append(LoggingEvent loggingEvent) {
                String msg = loggingEvent.getMessage().toString();
                if (msg != null && msg.contains("next")) {
                    System.out.println("-----> message: " + msg);
                    counts[0]++;
                }
            }

            @Override
            public void close() {

            }

            @Override
            public boolean requiresLayout() {
                return false;
            }
        };

        logger.removeAllAppenders();
        logger.setAdditivity(false);
        logger.addAppender(appender);
        logger.setLevel(Level.DEBUG);


        Connection connection = manager.getConnectNotNull();
        TableName tableName = TableName.valueOf("ods:test_1");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        scan.setCaching(caching);
        scan.setBatch(batch);
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) {
            counts[1]++;
        }
        results.close();

        System.out.println("-----> Caching: " + caching + "    Batch: " + batch + "    result: " + counts[1] + "    RPC: " + counts[0]);

        table.close();
        manager.closeConnect();
    }

    private static void incrementTest(HbaseManager manager) throws IOException {
        Set<HColumnBean> beans = new HashSet<>();
        HColumnBean bean = new HColumnBean();
        bean.family = "daily";
        bean.qualifier = "pv";
        bean.value = "100";

        HColumnBean bean2 = new HColumnBean();
        bean2.family = "daily";
        bean2.qualifier = "uv";
        bean2.value = "90";

        beans.add(bean);
        beans.add(bean2);

        manager.increment("my_app:tb_daily", "a20180725", beans);
    }

    private static void checkAndDeleteTest(HbaseManager manager) throws IOException {
        HColumnBean bean = new HColumnBean();
        bean.family = "info";
        bean.qualifier = "name";
        bean.value = "caocao";
        manager.checkAndDeleteOne("ods:test_1", "10001000", bean);
    }

    private static void scanFilterListTest(HbaseManager manager) throws IOException {
        FilterList filters = new FilterList();
        Filter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("info")));
        Filter filter1 = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("zh")));
        filters.addFilter(filter);
        filters.addFilter(filter1);
        Map<String, Set<HColumnBean>> map = manager.scan("sit_my_account:user_asset", "0001", "1000", filters);
        System.out.println(map);
    }

    private static void scanRowFilterTest(HbaseManager manager) throws IOException {
        Filter filter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("00010001")));
        Map<String, Set<HColumnBean>> map = manager.scan("sit_my_account:user_asset", "0001", "00010003", filter);
        System.out.println(map);
    }

    private static void scanPrefixFilterTest(HbaseManager manager) throws IOException {
        Filter filter = new PrefixFilter(Bytes.toBytes("0001"));
        Map<String, Set<HColumnBean>> map = manager.scan("sit_my_account:user_asset", "0001", "00010003", filter);
        System.out.println(map);
    }

    private static void scanAllTest(HbaseManager manager) throws IOException {
        // Map<String, Set<HColumnBean>> map = manager.scan("sit_my_account:user_asset", "0", "00010003");
        // System.out.println(map);

        // scan all
        Map<String, Set<HColumnBean>> map = manager.scan("sit_my_account:user_asset", "", "");
        System.out.println(map);

        // scan all
        // Map<String, Set<HColumnBean>> map = manager.scanAll("sit_my_account:user_asset");
        // System.out.println(map);
    }

    private static void getRowKeyValueFilterTest(HbaseManager manager) throws IOException {
        Filter filter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("zh")));
        Set<HColumnBean> results = manager.getByRowKey("sit_my_account:user_asset", "00010001", filter);
        System.out.println(results);
    }

    private static void getRowKeyQualifierFilterTest(HbaseManager manager) throws IOException {
        Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("name")));
        Set<HColumnBean> results = manager.getByRowKey("sit_my_account:user_asset", "00010001", filter);
        System.out.println(results);
    }

    private static void getRowKeyFamilyFilterTest(HbaseManager manager) throws IOException {
        Filter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("info")));
        Set<HColumnBean> results = manager.getByRowKey("sit_my_account:user_asset", "00010001", filter);
        System.out.println(results);
    }

    private static void getRowKeyTest(HbaseManager manager) throws IOException {
        Set<HColumnBean> results = manager.getByRowKey("sit_my_account:user_asset", "00010001", null);
        results.forEach(bean -> System.out.println(bean.toString()));
    }


    private static void deleteMutilsTest(HbaseManager manager) throws IOException {
        Map<String, Set<HColumnBean>> maps = new HashMap<>();
        Set<HColumnBean> beans = new HashSet<>();
        HColumnBean bean = new HColumnBean();
        bean.family = "info";
        bean.qualifier = "age";
        beans.add(bean);
        maps.put("00010002", beans);


        Set<HColumnBean> beans2 = new HashSet<>();
        HColumnBean bean2 = new HColumnBean();
        bean2.family = "info1";
        bean2.qualifier = "country";
        beans2.add(bean2);
        maps.put("00010001", beans2);

        manager.deleteMutils("sit_my_account:user_asset", maps);
    }

    private static void deleteRowKeyTest(HbaseManager manager) throws IOException {
        manager.deleteRowKey("sit_my_account:user_asset", "00010003");
    }

    private static void deleteOneTest(HbaseManager manager) throws IOException {
        Set<HColumnBean> beans = new HashSet<HColumnBean>();
        HColumnBean bean = new HColumnBean();
        bean.family = "info";
        bean.qualifier = "name";
        beans.add(bean);
        manager.deleteOne("sit_my_account:user_asset", "00010002", beans);
    }

    private static void putListTest(HbaseManager manager) throws IOException {
        Map<String, Set<HColumnBean>> maps = new HashMap<String, Set<HColumnBean>>();

        // -------------------- //
        Set<HColumnBean> beans = new HashSet<HColumnBean>();
        HColumnBean bean1 = new HColumnBean();
        bean1.family = "info";
        bean1.qualifier = "name";
        bean1.value = "liubei";
        beans.add(bean1);

        HColumnBean bean2 = new HColumnBean();
        bean2.family = "info";
        bean2.qualifier = "age";
        bean2.value = "28";
        beans.add(bean2);

        HColumnBean bean3 = new HColumnBean();
        bean3.family = "info";
        bean3.qualifier = "sex";
        bean3.value = "male";
        beans.add(bean3);

        // --------------------- //
        Set<HColumnBean> beans2 = new HashSet<HColumnBean>();
        HColumnBean bean21 = new HColumnBean();
        bean21.family = "info";
        bean21.qualifier = "name";
        bean21.value = "guanyu";
        beans2.add(bean21);

        HColumnBean bean22 = new HColumnBean();
        bean22.family = "info";
        bean22.qualifier = "age";
        bean22.value = "27";
        beans2.add(bean22);

        HColumnBean bean23 = new HColumnBean();
        bean23.family = "info";
        bean23.qualifier = "sex";
        bean23.value = "male";
        beans2.add(bean23);

        maps.put("00010002", beans);
        maps.put("00010003", beans2);

        manager.putMutis("sit_my_account:user_asset", maps);
    }

    private static void putOneTest(HbaseManager manager) throws IOException {
        Set<HColumnBean> beans = new HashSet<HColumnBean>();
        HColumnBean bean = new HColumnBean();
        bean.family = "info";
        bean.qualifier = "name";
        bean.value = "zhang";

        HColumnBean bean1 = new HColumnBean();
        bean1.family = "info";
        bean1.qualifier = "age";
        bean1.value = "28";

        HColumnBean bean2 = new HColumnBean();
        bean2.family = "info1";
        bean2.qualifier = "country";
        bean2.value = "CN";

        beans.add(bean);
        beans.add(bean1);
        beans.add(bean2);

        manager.putOne("sit_my_account:user_asset", "00010001", beans);
    }

    private static void createTableTest(HbaseManager manager) throws IOException {
        HColumnDescriptorBean bean = new HColumnDescriptorBean();
        bean.family = "info";
        bean.inMemery = true;
        bean.maxVersion = 3;
        bean.minVersion = 1;
        bean.compressionType = Compression.Algorithm.SNAPPY;

        List<HColumnDescriptorBean> beans = new ArrayList<HColumnDescriptorBean>();
        beans.add(bean);


        HTableDescriptorBean tableBean = new HTableDescriptorBean();
        tableBean.tbName = "invt_borrows_accept_paid";
        tableBean.namespace = "stb9_db_nono";
        tableBean.isExistDelete = false;
        tableBean.beans = beans;
        manager.createTable(tableBean);
    }

    private static void listTableTest(HbaseManager manager) throws IOException {
        List<String> listTables = manager.listTables();
        for (String table : listTables) {
            if (table.startsWith("sit_") || table.startsWith("stb9_")) {
                System.out.println(table);
            }
        }
    }
}
