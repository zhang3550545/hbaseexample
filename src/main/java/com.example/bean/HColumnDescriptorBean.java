package com.example.bean;

import org.apache.hadoop.hbase.io.compress.Compression;

import java.io.Serializable;

public class HColumnDescriptorBean implements Serializable {
    public String family;
    public int maxVersion = 1;
    public int minVersion = 1;
    public boolean inMemery;
    public Compression.Algorithm compressionType = Compression.Algorithm.NONE;
}
