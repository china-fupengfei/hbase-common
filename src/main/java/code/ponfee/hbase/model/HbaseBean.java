package code.ponfee.hbase.model;

import java.beans.Transient;
import java.io.Serializable;

import code.ponfee.hbase.HbaseDao;

/**
 * Base mapped by hbase table
 * 
 * @author Ponfee
 */
public interface HbaseBean<R extends Comparable<? super R> & Serializable>
    extends Comparable<HbaseBean<R>>, Serializable {

    /**
     * Returns the hbase row key
     * 
     * @return a hbase row key
     */
    R getRowKey();

    /**
     * Returns the row number for current page result
     * 
     * @return a int row number of page
     */
    int getRowNum();

    /**
     * Sets row key to hbase bean
     * 
     * @param rowKey the hbase row key
     */
    void setRowKey(R rowKey);

    /**
     * Sets row number for page data list
     * 
     * @param rowNum the current page data list row number
     */
    void setRowNum(int rowNum);

    //int getTimestamp();
    //int getSequenceId();

    /**
     * Returns the data object hbase rowkey, 
     * sub class can override this methods
     * 
     * @return a rowkey
     */
    default R buildRowKey() {
        return this.getRowKey();
    }

    /**
     * Sub class can override this method
     * 
     * @return row key as string
     */
    @Transient
    default String getRowKeyAsString() {
        R rowKey;
        return (rowKey = getRowKey()) == null
               ? null : rowKey.toString();
    }

    /**
     * Sub class can override this method
     * 
     * @return row key as string
     */
    @Transient
    default byte[] getRowKeyAsBytes() {
        return HbaseDao.toBytes(getRowKey());
    }

    // -------------------------------------------Comparable & Object
    default @Override int compareTo(HbaseBean<R> other) {
        R tkey, okey;
        if ((tkey = this.getRowKey()) == null) {
            return 1; // null last
        } else if (other == null
            || (okey = other.getRowKey()) == null) {
            return -1;
        } else {
            return tkey.compareTo(okey);
        }
        /*return new CompareToBuilder()
            .append(this.getRowKey(), other.getRowKey())
            .toComparison();*/
    }

    default int hashCode0() {
        R rowKey;
        if ((rowKey = this.getRowKey()) == null) {
            return 0;
        } else {
            return rowKey.hashCode();
        }
        /*return new HashCodeBuilder()
            .append(this.getRowKey())
            .toHashCode();*/
    }

    @SuppressWarnings("unchecked")
    default boolean equals0(Object obj) {
        if (!(obj instanceof HbaseBean)) {
            return false;
        }

        R tkey, okey;
        if ((tkey = this.getRowKey()) == null
            || (okey = ((HbaseBean<R>) obj).getRowKey()) == null) {
            return false;
        } else {
            return tkey.equals(okey);
        }
        /*return new EqualsBuilder()
                .append(this.getRowKey(), ((HbaseMap<?, R>) obj)
                .getRowKey()).isEquals();*/
    }

    default String toString0() {
        return getClass().getName() + "@" + this.getRowKey();
        //return new ToStringBuilder(this).toString();
    }

}
