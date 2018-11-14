package code.ponfee.hbase.model;

import java.beans.Transient;
import java.io.Serializable;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

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
    public int compareTo(HbaseEntity<R> o) {
        if (o == null) {
            return 1;
        }
        return new CompareToBuilder().append(rowKey, o.rowKey)
                                     .toComparison();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(rowKey).toHashCode();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HbaseEntity)) {
            return false;
        }
        return new EqualsBuilder()
                .append(rowKey, ((HbaseEntity<R>) obj).rowKey)
                .isEquals();
    }

    @Override
    public String toString() {
        return getClass().getName() + "@" + rowKey;
    }

}
