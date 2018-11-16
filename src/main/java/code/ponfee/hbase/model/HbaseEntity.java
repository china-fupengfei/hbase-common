package code.ponfee.hbase.model;

import java.beans.Transient;
import java.io.Serializable;

import code.ponfee.hbase.annotation.HbaseField;

/**
 * Base Entity Class for hbase
 * 
 * @author Ponfee
 */
public abstract class HbaseEntity<R extends Serializable & Comparable<? super R>>
    implements HbaseBean<R> {

    private static final long serialVersionUID = 2467942701509706341L;

    @HbaseField(ignore = true)
    protected R rowKey;

    @HbaseField(ignore = true)
    protected int rowNum;

    /*@HbaseField(ignore = true)
    protected int sequenceId;
    @HbaseField(ignore = true)
    protected int timestamp;*/

    /**
     * Returns the data object hbase rowkey, 
     * sub class can override this methods
     * 
     * @return a rowkey
     */
    public R buildRowKey() {
        return this.getRowKey();
    }

    /**
     * Sub class can override this method
     * 
     * @return row key as string
     */
    public @Transient String getRowKeyAsString() {
        return rowKey == null ? null : rowKey.toString();
    }

    public @Override final R getRowKey() {
        return rowKey;
    }

    public @Override final int getRowNum() {
        return rowNum;
    }

    public void setRowKey(R rowKey) {
        this.rowKey = rowKey;
    }

    public void setRowNum(int rowNum) {
        this.rowNum = rowNum;
    }

    public @Override int hashCode() {
        return HbaseBean.super.hashCode0();
    }

    public @Override boolean equals(Object obj) {
        return HbaseBean.super.equals0(obj);
    }

    public @Override String toString() {
        return HbaseBean.super.toString0();
    }

}
