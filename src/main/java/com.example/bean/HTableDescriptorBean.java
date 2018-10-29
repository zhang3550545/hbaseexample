package com.example.bean;

import java.io.Serializable;
import java.util.List;

public class HTableDescriptorBean implements Serializable {
    public String tbName;
    public String namespace;
    public boolean isExistDelete;
    public List<HColumnDescriptorBean> beans;
}
