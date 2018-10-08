package code.ponfee.hbase.model;

import java.util.HashMap;

/**
 * Class HbaseMap mapping for hbase table
 * 
 * @author Ponfee
 */
public abstract class HbaseMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = 2482090979352032846L;

    /** The hbase row key name */
    public static final String ROW_KEY_NAME = "rowKey";
    public static final String ROW_NUM_NAME = "rowNum";
}
