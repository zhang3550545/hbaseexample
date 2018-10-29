package com.example.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class CustomFilter extends FilterBase {

    private boolean filterRow = true;
    private byte[] value;

    public CustomFilter(byte[] value) {
        this.value = value;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) {
        if (Bytes.compareTo(value, v.getValue()) == 0) {
            filterRow = false;
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRow() {
        return filterRow;
    }

    @Override
    public void reset() {
        // 重置标记位
        filterRow = true;
    }
}
