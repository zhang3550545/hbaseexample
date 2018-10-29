package com.example.bean;

import java.io.Serializable;

public class HColumnBean implements Serializable {
    public String family;
    public String qualifier;
    public String value;
    public long ts = 0L;

    @Override
    public String toString() {
        return "family: " + family + "   qualifier: " + qualifier + "   value: " + value + "   ts: " + ts;
    }
}
