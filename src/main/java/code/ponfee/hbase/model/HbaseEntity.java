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
    implements Serializable, Comparable<HbaseEntity<R>> {

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

    public R getRowKey() {
        return rowKey;
    }

    public int getRowNum() {
        return rowNum;
    }

    public void setRowKey(R rowKey) {
        this.rowKey = rowKey;
    }

    public void setRowNum(int rowNum) {
        this.rowNum = rowNum;
    }

    @Override
    public int compareTo(HbaseEntity<R> other) {
        if (this.rowKey == null) {
            return 1; // natural order: null as last
        } else if (other == null || other.rowKey == null) {
            return -1;
        } else {
            return this.rowKey.compareTo(other.rowKey);
        }
    }

    @Override
    public int hashCode() {
        if (this.rowKey == null) {
            return 0;
        } else {
            return this.rowKey.hashCode();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HbaseEntity)) {
            return false;
        }

        HbaseEntity<R> other;
        if (this.rowKey == null 
            || (other = (HbaseEntity<R>) obj).rowKey == null) {
            return false;
        }
        return this.rowKey.equals(other.rowKey);
    }

    @Override
    public String toString() {
        return getClass().getName() + "@" + rowKey;
    }

}
