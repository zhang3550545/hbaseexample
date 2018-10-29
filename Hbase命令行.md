## Hbase命令行

#### 1.创建表

```
# 简单的创建，包含表名，列族名称
create 'test_1',{NAME => 'info'}


# 如果需要创建包含命名空间的表，需要先创建命名空间
# 创建命名空间
create_namespace "ods"
# 创建包含命名空间的表  注意：如果不创建命名空间，直接执行会报错
create 'ods:test_1',{NAME => 'info'}


# 在创建表是添加一些参数，
# VERSIONS：设置保存的版本数
# MIN_VERSIONS：最小存储版本数
# IN_MEMORY：设置激进缓存，优先考虑将该列族放入块缓存中
# COMPRESSION：设置压缩算法
create 'ods:test_2',{NAME => 'info',VERSIONS => 3,MIN_VERSIONS => 1,IN_MEMORY => true,COMPRESSION => 'LZO'}


# 创建多个列族
create 'test_3',{NAME => 'f1',VERSION => 1},{NAME => 'f2',VERSION => 1},{NAME => 'f3',VERSION => 1}
```

#### 2.查看表信息
```
# 列出所有表
list

# 列出单个表
list 'test_1'


# 查看表的详情，可以查看到表是否Disable，表的描述信息，还有版本信息
desc 'test_1'
```


#### 3.删除表
```
# 在删除表之前，需要先disable

disable 'test_1'

drop 'test_1'

# 如果disable后，没有删除，表不可用，需要enable
enable 'test_1'
```

#### 4.修改表

```
# 添加列族
alter 'ods:test_1',{NAME => 'info_1'}

# 修改表的版本信息
alter 'ods:test_1',{NAME => 'info_1', VERSIONS => 3}
或者
alter 'ods:test_1', NAME => 'info_1', VERSIONS => 4

# 修改多个描述信息
alter 'ods:test_1', {NAME => 'info_1', COMPRESSION => 'LZO', IN_MEMORY => true}

# 修改多个列族的描述信息
alter 'ods:test_1', {NAME => 'info', COMPRESSION => 'LZO', IN_MEMORY => true}，{NAME => 'info_1',VERSIONS => 5}

# 删除列族
alter 'ods:test_1' , 'delete' => 'info_1'

```


#### 5.插入数据

```
# put 语句
hbase> put 'ns1:t1', 'r1', 'c1', 'value'
hbase> put 't1', 'r1', 'c1', 'value'
hbase> put 't1', 'r1', 'c1', 'value', ts1
hbase> put 't1', 'r1', 'c1', 'value', {ATTRIBUTES=>{'mykey'=>'myvalue'}}
hbase> put 't1', 'r1', 'c1', 'value', ts1, {ATTRIBUTES=>{'mykey'=>'myvalue'}}
hbase> put 't1', 'r1', 'c1', 'value', ts1, {VISIBILITY=>'PRIVATE|SECRET'}
```

插入测试数据

```
put 'ods:test_1', '100010001','info:name','liubei'
put 'ods:test_1', '100010001','info:age','30'
put 'ods:test_1', '100010001','info:sex','male'
put 'ods:test_1', '100010001','info:country','shu'

put 'ods:test_1', '100010002','info:name','guanyu'
put 'ods:test_1', '100010002','info:age','28'
put 'ods:test_1', '100010002','info:sex','male'
put 'ods:test_1', '100010002','info:country','shu'

put 'ods:test_1', '100010003','info:name','zhangfei'
put 'ods:test_1', '100010003','info:age','27'
put 'ods:test_1', '100010003','info:sex','male'
put 'ods:test_1', '100010003','info:country','shu'

put 'ods:test_1', '100010004','info:name','caocao'
put 'ods:test_1', '100010004','info:age','32'
put 'ods:test_1', '100010004','info:sex','male'
put 'ods:test_1', '100010004','info:country','wei'

put 'ods:test_1', '100010005','info:name','sunshangxiang'
put 'ods:test_1', '100010005','info:age','18'
put 'ods:test_1', '100010005','info:sex','female'
put 'ods:test_1', '100010005','info:country','wu'

put 'ods:test_1', '100010005','info1:desc','beautiful'

```

如果需要修改数据，使用put语句进行覆盖就好。

#### 6.删除数据

```
delete 语句
hbase> delete 'ns1:t1', 'r1', 'c1', ts1
hbase> delete 't1', 'r1', 'c1', ts1
hbase> delete 't1', 'r1', 'c1', ts1, {VISIBILITY=>'PRIVATE|SECRET'}
```

示例：

```
# 清除数据
truncate 'ods:test_1'

# 通过rowkey删除数据
delete 'ods:test_1','100010005'

# 删除指定列族数据
delete 'ods:test_1','100010005','info:sex'
```

#### 7.查询数据

- get语句

```
hbase> get 'ns1:t1', 'r1'
hbase> get 't1', 'r1'
hbase> get 't1', 'r1', {TIMERANGE => [ts1, ts2]}
hbase> get 't1', 'r1', {COLUMN => 'c1'}
hbase> get 't1', 'r1', {COLUMN => ['c1', 'c2', 'c3']}
hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1}
hbase> get 't1', 'r1', {COLUMN => 'c1', TIMERANGE => [ts1, ts2], VERSIONS => 4}
hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1, VERSIONS => 4}
hbase> get 't1', 'r1', {FILTER => "ValueFilter(=, 'binary:abc')"}
hbase> get 't1', 'r1', 'c1'
hbase> get 't1', 'r1', 'c1', 'c2'
hbase> get 't1', 'r1', ['c1', 'c2']
hbase> get 't1', 'r1', {COLUMN => 'c1', ATTRIBUTES => {'mykey'=>'myvalue'}}
hbase> get 't1', 'r1', {COLUMN => 'c1', AUTHORIZATIONS => ['PRIVATE','SECRET']}
hbase> get 't1', 'r1', {CONSISTENCY => 'TIMELINE'}
hbase> get 't1', 'r1', {CONSISTENCY => 'TIMELINE', REGION_REPLICA_ID => 1}
```

示例：

```
# 直接通过rowkey查询
get 'ods:test_1','100010005'

# 通过column字段查询，查询 info 列族的数据
get 'ods:test_1','100010005',{COLUMN => 'info'}

# 查询 info 列族的数据中的name字段数据
get 'ods:test_1','100010005',{COLUMN => 'info:name'}

# 查询多个列族数据
get 'ods:test_1','100010005',{COLUMN => ['info','info1']}

get 'ods:test_1','100010005',{COLUMN => ['info:name','info1:desc']}

# 指定时间查询
get 'ods:test_1','100010005',{COLUMN => 'info',TIMESTAMP =>1532495228577}

# 指定版本查询
get 'ods:test_1','100010005',{COLUMN => 'info',VERSIONS => 2}
```

- scan语句

```
hbase> scan 'ns1:t1', {COLUMNS => ['c1', 'c2'], LIMIT => 10, STARTROW => 'xyz'}
hbase> scan 't1', {COLUMNS => ['c1', 'c2'], LIMIT => 10, STARTROW => 'xyz'}
hbase> scan 't1', {COLUMNS => 'c1', TIMERANGE => [1303668804, 1303668904]}
hbase> scan 't1', {REVERSED => true}
hbase> scan 't1', {ALL_METRICS => true}
hbase> scan 't1', {METRICS => ['RPC_RETRIES', 'ROWS_FILTERED']}
hbase> scan 't1', {ROWPREFIXFILTER => 'row2', FILTER => "(QualifierFilter (>=, 'binary:xyz')) AND (TimestampsFilter ( 123, 456))"}
hbase> scan 't1', {FILTER => org.apache.hadoop.hbase.filter.ColumnPaginationFilter.new(1, 0)}
hbase> scan 't1', {CONSISTENCY => 'TIMELINE'}
...
```

示例：

```
# 扫描整张表
scan 'ods:test_1'

# 指定rowkey查询，限制条数
scan 'ods:test_1',{STARTROW => '100010001', LIMIT => 3}

# 指定rowkey的范围
scan 'ods:test_1',{STARTROW => '100010001', STOPROW => '100010005'}

# 反转
scan 'ods:test_1',{STARTROW => '100010005',STOPROW => '100010001', REVERSED => true}

# 指定列名
scan 'ods:test_1',{COLUMN => 'info'}
scan 'ods:test_1',{COLUMN => 'info:name'}
scan 'ods:test_1',{COLUMN => 'info:name', STARTROW => '100010001',STOPROW => '100010003'}

# 指定多个列名
scan 'ods:test_1',{COLUMN => ['info:name','info:age']}



# 过滤器，在实际的使用中，记得指定rowkey的范围

# 前缀过滤器
scan 'ods:test_1', FILTER => "PrefixFilter ('10001')"

# 列名前缀过滤器
scan 'ods:test_1', FILTER => "ColumnPrefixFilter ('na')"

# 值过滤器
# "ValueFilter (=,'binary:liubei')"  其中binary:liubei表示匹配的二进制内容，=表示等于，还可以>=,<=,>,<
scan 'ods:test_1', FILTER => "ValueFilter (=,'binary:liubei')"
scan 'ods:test_1', FILTER => "ValueFilter (>=,'binary:zhangfei')"
# substring表示value截取内容包含'zhang'字符串
scan 'ods:test_1', FILTER => "ValueFilter (=,'substring:zhang')"

# FamilyFilter，用法同ValueFilter
scan 'ods:test_1', FILTER => "FamilyFilter (=,'binary:info')"

# RowFilter
scan 'ods:test_1', FILTER => "RowFilter (!=,'binary:100010003')"

# 多个过滤器混合使用
scan 'ods:test_1', FILTER => "RowFilter (!=,'binary:100010003') AND ValueFilter (=,'binary:liubei')"

scan 'ods:test_1', FILTER => "ColumnPrefixFilter ('na') AND ValueFilter (=,'substring:zhang')"
```

#### 8.计数器操作


```
hbase> incr 'ns1:t1', 'r1', 'c1'
hbase> incr 't1', 'r1', 'c1'
hbase> incr 't1', 'r1', 'c1', 1
hbase> incr 't1', 'r1', 'c1', 10
hbase> incr 't1', 'r1', 'c1', 10, {ATTRIBUTES=>{'mykey'=>'myvalue'}}
hbase> incr 't1', 'r1', 'c1', {ATTRIBUTES=>{'mykey'=>'myvalue'}}
hbase> incr 't1', 'r1', 'c1', 10, {VISIBILITY=>'PRIVATE|SECRET'}
```

与put操作类似。

示例：
```
# 插入daily:pv,日页面访问量
incr 'my_app:tb_daily','a20180725','daily:pv',10000

# 插入daily:uv,日用户访问量
incr 'my_app:tb_daily','a20180725','daily:uv',9000
```

