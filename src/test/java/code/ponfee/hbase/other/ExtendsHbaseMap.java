package code.ponfee.hbase.other;

import code.ponfee.hbase.annotation.HbaseTable;
import code.ponfee.hbase.model.HbaseMap;

@HbaseTable(namespace="default", tableName="t_test_fonfee", family="cf1")
public class ExtendsHbaseMap<V> extends HbaseMap<V> {

    private static final long serialVersionUID = 1L;

}
