package com.example;

import com.example.bean.HColumnBean;
import com.example.bean.HColumnDescriptorBean;
import com.example.bean.HTableDescriptorBean;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.*;


public class HbaseManager {

    private static HbaseManager hbaseManager;
    private static Connection connection;

    private HbaseManager() {
        try {
            connection = getConnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HbaseManager getInstance() {
        if (hbaseManager == null) {
            synchronized (HbaseManager.class) {
                if (hbaseManager == null) {
                    hbaseManager = new HbaseManager();
                }
            }
        }
        return hbaseManager;
    }

    /**
     * 获取连接
     */
    private Connection getConnect() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
//        configuration.set(HConstants.ZOOKEEPER_QUORUM, "localhost:2181");
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
        return ConnectionFactory.createConnection(configuration);
    }

    /**
     * 获取Connect对象，做了安全的检测
     */
    public Connection getConnectNotNull() throws IOException {
        if (connection == null || connection.isClosed() || connection.isAborted()) {
            connection = getConnect();
        }
        return connection;
    }

    /**
     * 创建表
     */
    private void createTable(String tbName, boolean isExistDelete, List<HColumnDescriptorBean> beans) throws IOException {
        getConnectNotNull();
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tbName);
        // 创建 namespace
        String namespace = tableName.getNamespaceAsString();
        if (Strings.isNotEmpty(namespace)) {
            if (!checkNameSpaceIsExist(namespace, admin)) {
                admin.createNamespace(NamespaceDescriptor.create(namespace).build());
            }
        }
        // 创建表
        if (isExistDelete && admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        checkParamsIsNullThrow(beans, true);
        // 创建表的表述对象
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        for (HColumnDescriptorBean bean : beans) {
            checkParamsIsNullThrow(bean);
            String family = bean.family;
            checkParamsIsNullThrow(family);
            // 创建列的描述对象
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            // 添加描述
            columnDescriptor.setInMemory(bean.inMemery);
            columnDescriptor.setMinVersions(bean.minVersion);
            columnDescriptor.setMaxVersions(bean.maxVersion);
            columnDescriptor.setCompressionType(bean.compressionType);
            // 将列的描述添加到表中
            tableDescriptor.addFamily(columnDescriptor);
        }
        // 创建表
        admin.createTable(tableDescriptor);
        admin.close();
    }

    /**
     * 创建表
     */
    public void createTable(HTableDescriptorBean tableBean) throws IOException {
        checkParamsIsNull(tableBean);
        String tbName = tableBean.tbName;
        String namespace = tableBean.namespace;
        tbName = getTableName(namespace, tbName);
        createTable(tbName, tableBean.isExistDelete, tableBean.beans);
    }

    /**
     * 删除表
     */
    public void deleteTable(String namespace, String tbName) throws IOException {
        String tableName = getTableName(namespace, tbName);
        deleteTable(tableName);
    }

    /**
     * 删除表
     */
    public void deleteTable(String tbName) throws IOException {
        getConnectNotNull();
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tbName);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.close();
    }

    /**
     * 添加多个列族
     */
    public void alterAddFamilys(String tbName, List<HColumnDescriptorBean> beans) throws IOException {
        getConnectNotNull();
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tbName);
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
        checkParamsIsNull(beans);
        for (HColumnDescriptorBean bean : beans) {
            setFamily(bean, tableDescriptor);
        }
        admin.close();
    }

    /**
     * 添加 列族
     */
    public void alterAddFamily(String tbName, HColumnDescriptorBean bean) throws IOException {
        getConnectNotNull();
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tbName);
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
        setFamily(bean, tableDescriptor);
        admin.close();
    }


    private void setFamily(HColumnDescriptorBean bean, HTableDescriptor tableDescriptor) {
        checkParamsIsNull(bean);
        checkParamsIsNull(tableDescriptor);

        HColumnDescriptor family = tableDescriptor.getFamily(Bytes.toBytes(bean.family));
        if (family == null) {
            family = new HColumnDescriptor(bean.family);
            family.setMinVersions(bean.minVersion);
            family.setMaxVersions(bean.maxVersion);
            family.setCompressionType(bean.compressionType);
            family.setInMemory(bean.inMemery);
            tableDescriptor.addFamily(family);
        }
    }

    /**
     * 修改列 column 的描述信息
     */
    public void alterColumnDescriptor(String tbName, HColumnDescriptorBean bean) throws IOException {
        getConnectNotNull();
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tbName);
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
        HColumnDescriptor family = tableDescriptor.getFamily(Bytes.toBytes(bean.family));
        checkParamsIsNull(family);
        family.setMinVersions(bean.minVersion);
        family.setMaxVersions(bean.maxVersion);
        family.setCompressionType(bean.compressionType);
        family.setInMemory(bean.inMemery);
        admin.close();
    }

    /**
     * 修改列 column 的描述信息
     */
    public void alterColumnDescriptor(String namespace, String tbName, HColumnDescriptorBean bean) throws IOException {
        tbName = getTableName(namespace, tbName);
        alterColumnDescriptor(tbName, bean);
    }

    /**
     * 列出所有表名
     */
    public List<String> listTables() throws IOException {
        getConnectNotNull();
        Admin admin = connection.getAdmin();
        TableName[] tableNames = admin.listTableNames();
        List<String> list = new ArrayList<String>();
        if (checkParamsIsNull(tableNames)) {
            for (TableName tb : tableNames) {
                list.add(new String(tb.getName()));
            }
        }
        return list;
    }


    public void putOne(String tbName, String rowkey, Set<HColumnBean> beans) throws IOException {
        getConnectNotNull();
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        checkEnableTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        checkParamsIsNullThrow(beans, true);
        for (HColumnBean bean : beans) {
            checkParamsIsNullThrow(bean);
            put.addColumn(Bytes.toBytes(bean.family), Bytes.toBytes(bean.qualifier), bean.ts, Bytes.toBytes(bean.value));
        }
        table.put(put);
        table.close();
    }

    public void putMutis(String tbName, Map<String, Set<HColumnBean>> maps) throws IOException {
        getConnectNotNull();
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        checkEnableTable(tableName);
        checkParamsIsNullThrow(maps, true);
        List<Put> puts = new ArrayList<Put>();
        for (Map.Entry<String, Set<HColumnBean>> map : maps.entrySet()) {
            String rowkey = map.getKey();
            checkParamsIsNullThrow(rowkey);
            Set<HColumnBean> beans = map.getValue();
            checkParamsIsNullThrow(beans, true);
            Put put = new Put(Bytes.toBytes(rowkey));
            for (HColumnBean bean : beans) {
                checkParamsIsNullThrow(bean);
                put.addColumn(Bytes.toBytes(bean.family), Bytes.toBytes(bean.qualifier), bean.ts, Bytes.toBytes(bean.value));
            }
            puts.add(put);
        }
        table.put(puts);
        table.close();
    }

    public void increment(String tbName, String rowkey, Set<HColumnBean> beans) throws IOException {
        getConnectNotNull();
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        checkEnableTable(tableName);
        checkParamsIsNullThrow(beans);
        Increment increment = new Increment(Bytes.toBytes(rowkey));
        for (HColumnBean bean : beans) {
            increment.addColumn(Bytes.toBytes(bean.family), Bytes.toBytes(bean.qualifier), Long.parseLong(bean.value));
        }
        Result result = table.increment(increment);
        checkParamsIsNullThrow(result);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            System.out.println("-----> cell: " + cell.toString() + "    value: " + Bytes.toLong(cell.getValue()));
        }
        table.close();
    }

    public void increment(String tbName, String rowkey, HColumnBean bean) throws IOException {
        getConnectNotNull();
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        checkEnableTable(tableName);
        checkParamsIsNullThrow(bean);
        long value = table.incrementColumnValue(Bytes.toBytes(rowkey), Bytes.toBytes(bean.family), Bytes.toBytes(bean.qualifier), Long.parseLong(bean.value));
        System.out.println("-----> value: " + value);
        table.close();
    }


    public void deleteRowKey(String tbName, String rowkey) throws IOException {
        getConnectNotNull();
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        table.close();
    }

    public void deleteOne(String tbName, String rowkey, Set<HColumnBean> beans) throws IOException {
        getConnectNotNull();
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        for (HColumnBean bean : beans) {
            checkParamsIsNullThrow(bean);
            if (bean.ts == 0L) {
                delete.addColumn(Bytes.toBytes(bean.family), Bytes.toBytes(bean.qualifier));
            } else {
                delete.addColumn(Bytes.toBytes(bean.family), Bytes.toBytes(bean.qualifier), bean.ts);
            }
        }
        table.delete(delete);
        table.close();
    }

    public void deleteMutils(String tbName, Map<String, Set<HColumnBean>> maps) throws IOException {
        getConnectNotNull();
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        checkEnableTable(tableName);
        checkParamsIsNullThrow(maps, true);
        List<Delete> deletes = new ArrayList<Delete>();
        for (Map.Entry<String, Set<HColumnBean>> map : maps.entrySet()) {
            String rowkey = map.getKey();
            checkParamsIsNullThrow(rowkey);
            Set<HColumnBean> beans = map.getValue();
            checkParamsIsNullThrow(beans, true);
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            for (HColumnBean bean : beans) {
                if (bean.ts == 0L) {
                    delete.addColumn(Bytes.toBytes(bean.family), Bytes.toBytes(bean.qualifier));
                } else {
                    delete.addColumn(Bytes.toBytes(bean.family), Bytes.toBytes(bean.qualifier), bean.ts);
                }
            }
            deletes.add(delete);
        }
        table.delete(deletes);
        table.close();
    }

    public void checkAndDeleteOne(String tbName, String rowkey, HColumnBean bean) throws IOException {
        getConnectNotNull();
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.checkAndDelete(Bytes.toBytes(rowkey), Bytes.toBytes(bean.family), Bytes.toBytes(bean.qualifier), Bytes.toBytes(bean.value), delete);
        table.close();
    }

    public Set<HColumnBean> getByRowKey(String tbName, String rowkey, Filter filter) throws IOException {
        getConnectNotNull();
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        checkEnableTable(tableName);
        Get get = new Get(Bytes.toBytes(rowkey));
        if (filter != null) {
            get.setFilter(filter);
        }
        Result result = table.get(get);
        Set<HColumnBean> set = getColumnBeans(result);
        table.close();
        return set;
    }

    public Map<String, Set<HColumnBean>> scanAll(String tbName) throws IOException {
        getConnectNotNull();
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        checkEnableTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Map<String, Set<HColumnBean>> map = new HashMap<>();
        for (Result result : scanner) {
            String rowkey = Bytes.toString(result.getRow());
            Set<HColumnBean> beans = getColumnBeans(result);
            map.put(rowkey, beans);
        }
        table.close();
        return map;
    }

    public Map<String, Set<HColumnBean>> scan(String tbName, String startRowKey, String stopRowKey) throws IOException {
        getConnectNotNull();
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        checkEnableTable(tableName);
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRowKey));
        scan.setStopRow(Bytes.toBytes(stopRowKey));

        ResultScanner scanner = table.getScanner(scan);
        Map<String, Set<HColumnBean>> map = new HashMap<>();
        for (Result result : scanner) {
            String rowkey = Bytes.toString(result.getRow());
            Set<HColumnBean> beans = getColumnBeans(result);
            map.put(rowkey, beans);
        }
        table.close();
        return map;
    }

    public Map<String, Set<HColumnBean>> scan(String tbName, String startRowKey, String stopRowKey, Filter filter) throws IOException {
        getConnectNotNull();
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        checkEnableTable(tableName);
        Scan scan = new Scan();
        if (checkParamsIsNull(filter)) {
            scan.setFilter(filter);
        }
        scan.setStartRow(Bytes.toBytes(startRowKey));
        scan.setStopRow(Bytes.toBytes(stopRowKey));
        ResultScanner scanner = table.getScanner(scan);
        Map<String, Set<HColumnBean>> map = new HashMap<>();
        for (Result result : scanner) {
            String rowkey = Bytes.toString(result.getRow());
            Set<HColumnBean> beans = getColumnBeans(result);
            map.put(rowkey, beans);
        }
        table.close();
        return map;
    }

    public Map<String, Set<HColumnBean>> scan(String tbName, int caching, int batch) throws IOException {
        getConnectNotNull();
        checkParamsIsNullThrow(tbName);
        TableName tableName = TableName.valueOf(tbName);
        Table table = connection.getTable(tableName);
        checkEnableTable(tableName);
        Scan scan = new Scan();
        scan.setCaching(caching);
        scan.setBatch(batch);
        ResultScanner scanner = table.getScanner(scan);
        Map<String, Set<HColumnBean>> map = new HashMap<>();
        for (Result result : scanner) {
            String rowkey = Bytes.toString(result.getRow());
            Set<HColumnBean> beans = getColumnBeans(result);
            map.put(rowkey, beans);
        }
        table.close();
        return map;
    }


    private Set<HColumnBean> getColumnBeans(Result result) {
        List<Cell> cells = result.listCells();
        checkParamsIsNullThrow(cells);
        Set<HColumnBean> beans = new HashSet<>();
        for (Cell cell : cells) {
            HColumnBean bean = new HColumnBean();
            bean.family = Bytes.toString(cell.getFamily());
            bean.qualifier = Bytes.toString(cell.getQualifier());
            bean.value = Bytes.toString(cell.getValue());
            bean.ts = cell.getTimestamp();
            beans.add(bean);
        }
        return beans;
    }

    private void checkEnableTable(TableName tableName) throws IOException {
        Admin admin = connection.getAdmin();
        if (!admin.isTableEnabled(tableName)) {
            admin.enableTable(tableName);
        }
    }


    public void closeConnect() throws IOException {
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 检测object是否为null，为null抛异常
     */
    public static void checkParamsIsNullThrow(Object obj) {
        if (obj == null)
            throw new IllegalArgumentException("obj is null");
    }

    /**
     * 检测string是否为null或空字符串，是则 抛异常
     */
    public static void checkParamsIsNullThrow(String str) {
        if (Strings.isEmpty(str))
            throw new IllegalArgumentException("str is empty");
    }

    /**
     * 检测list是否为null或size==0，是则 抛异常
     */
    public static void checkParamsIsNullThrow(Collection<?> list, boolean force) {
        if (list == null || (force && list.size() == 0))
            throw new IllegalArgumentException("list is null or size == 0");
    }


    public static void checkParamsIsNullThrow(Map<?, ?> map, boolean force) {
        if (map == null || (force && map.size() == 0))
            throw new IllegalArgumentException("list is null or size == 0");
    }

    /**
     * 判断是否为空，判断obj，str，list
     */
    public static boolean checkParamsIsNull(Object obj) {
        if (obj == null) return false;
        if (obj instanceof String && Strings.isEmpty((String) obj)) return false;
        if (obj instanceof Collection && ((Collection) obj).size() <= 0) return false;
        return true;
    }

    /**
     * 拼接 table name
     */
    public static String getTableName(String namespace, String tbName) {
        checkParamsIsNullThrow(tbName);
        if (Strings.isEmpty(namespace)) {
            return tbName;
        }
        return namespace + ":" + tbName;
    }

    /**
     * 返回 true 表示 namespace 存在，不需要创建
     */
    public static boolean checkNameSpaceIsExist(String namespace, Admin admin) {
        NamespaceDescriptor namespaceDescriptor = null;
        try {
            namespaceDescriptor = admin.getNamespaceDescriptor(namespace);
        } finally {
            return namespaceDescriptor != null && Strings.equals(namespace, namespaceDescriptor.getName());
        }
    }
}
