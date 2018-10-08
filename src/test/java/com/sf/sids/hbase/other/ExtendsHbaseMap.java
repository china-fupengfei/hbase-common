package com.sf.sids.hbase.other;

import com.sf.sids.hbase.annotation.HbaseTable;
import com.sf.sids.hbase.bean.HbaseMap;

@HbaseTable(namespace="default", tableName="t_test_fonfee", family="cf1")
public class ExtendsHbaseMap<V> extends HbaseMap<V> {

    private static final long serialVersionUID = 1L;

}
