package code.ponfee.hbase.model;

import java.io.Serializable;
import java.util.HashMap;

import code.ponfee.commons.math.Numbers;

/**
 * The Map class for mapped by hbase table
 * 
 * @author Ponfee
 * @param <V> the map value type(Object), only support 
 *            string value type(except ruwnum and rowkey)
 * @param <R> the row key type
 */
public abstract class HbaseMap<V, R extends Comparable<? super R> & Serializable>
    extends HashMap<String, V> implements HbaseBean<R> {

    private static final long serialVersionUID = 2482090979352032846L;

    /** The hbase row key name */
    public static final String ROW_KEY_NAME = "rowKey";
    public static final String ROW_NUM_NAME = "rowNum";
    //public static final String TIMESTAMP_NAME = "timestamp";
    //public static final String SEQUENCE_ID_NAME = "sequenceId";

    @Override @SuppressWarnings("unchecked")
    public final R getRowKey() {
        return (R) this.get(ROW_KEY_NAME);
    }

    @Override
    public final int getRowNum() {
        V rowNum = this.get(ROW_NUM_NAME);
        if (rowNum == null) {
            return 0;
        } else if (rowNum instanceof Number) {
            return ((Number) rowNum).intValue();
        } else {
            return Numbers.toInt(rowNum);
        }
    }

    @Override @SuppressWarnings("unchecked")
    public final void setRowKey(R rowKey) {
        this.put(ROW_KEY_NAME, (V) rowKey);
    }

    @Override @SuppressWarnings("unchecked")
    public final void setRowNum(int rowNum) {
        this.put(ROW_KEY_NAME, (V) (Integer) rowNum);
    }

    @Override
    public int hashCode() {
        return HbaseBean.super.hashCode0();
    }

    @Override
    public boolean equals(Object obj) {
        return HbaseBean.super.equals0(obj);
    }

    @Override
    public String toString() {
        return HbaseBean.super.toString0();
    }

}
