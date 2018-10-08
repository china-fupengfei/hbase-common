package code.ponfee.hbase.model;

import java.io.Serializable;
import java.util.Objects;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import code.ponfee.hbase.annotation.HbaseField;

/**
 * Base Entity Class for hbase
 * 
 * @author Ponfee
 */
public abstract class HbaseEntity
    implements Serializable, Comparable<HbaseEntity> {

    private static final long serialVersionUID = 2467942701509706341L;

    @HbaseField(ignore = true)
    protected String rowKey;

    @HbaseField(ignore = true)
    protected int rowNum;

    /*@HbaseField(ignore = true)
    protected int sequenceId;

    @HbaseField(ignore = true)
    protected int timestamp;*/

    /**
     * Returns the data object hbase rowkey
     * 
     * @return a string as rowkey
     */
    public abstract String buildRowKey();

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public int getRowNum() {
        return rowNum;
    }

    public void setRowNum(int rowNum) {
        this.rowNum = rowNum;
    }

    @Override
    public int compareTo(HbaseEntity o) {
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

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HbaseEntity)) {
            return false;
        }
        //return new EqualsBuilder().append(rowKey, ((Entity) obj).rowKey).isEquals();
        return Objects.equals(rowKey, ((HbaseEntity) obj).rowKey);
    }

    @Override
    public String toString() {
        return getClass().getName() + "@" + rowKey;
    }

}
