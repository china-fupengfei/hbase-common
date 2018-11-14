package code.ponfee.hbase.model;

import java.beans.Transient;
import java.io.Serializable;
import java.util.HashMap;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import code.ponfee.commons.math.Numbers;

/**
 * Class HbaseMap mapped by hbase table
 * 
 * @author Ponfee
 */
public abstract class HbaseMap<V, R extends Serializable & Comparable<? super R>>
    extends HashMap<String, V> implements Comparable<HbaseMap<V, R>> {

    private static final long serialVersionUID = 2482090979352032846L;

    /** The hbase row key name */
    public static final String ROW_KEY_NAME = "rowKey";
    public static final String ROW_NUM_NAME = "rowNum";
    //public static final String TIMESTAMP_NAME = "timestamp";
    //public static final String SEQUENCE_ID_NAME = "sequenceId";

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
        R rowKey = getRowKey();
        return rowKey == null ? null : rowKey.toString();
    }

    @SuppressWarnings("unchecked")
    public final R getRowKey() {
        return (R) this.get(ROW_KEY_NAME);
    }

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

    @Override
    public int compareTo(HbaseMap<V, R> o) {
        if (o == null) {
            return 1;
        }
        return new CompareToBuilder().append(this.getRowKey(), o.getRowKey())
                                     .toComparison();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.getRowKey()).toHashCode();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HbaseMap)) {
            return false;
        }
        return new EqualsBuilder()
                .append(this.getRowKey(), ((HbaseMap<?, R>) obj)
                .getRowKey()).isEquals();
    }

    @Override
    public String toString() {
        return getClass().getName() + "@" + this.getRowKey();
    }

}
