package code.ponfee.hbase.model;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.base.Preconditions;

import code.ponfee.commons.util.ObjectUtils;

/**
 * Page query for hbase
 * 
 * @author Ponfee
 */
public class PageQueryBuilder {

    private final int pageSize;
    private final PageSortOrder sortOrder;

    private boolean requireRowNum = true; // is include row number

    private Object startRow;
    private Boolean inclusiveStartRow;
    private Object stopRow;
    private Boolean inclusiveStopRow;

    private Object rowKeyPrefix;

    private String rowKeyRegexp; // regexp: only support string

    private Map<String, String[]> famQuaes;
    private int maxResultSize = -1;
    private boolean rowKeyOnly = false;

    private PageQueryBuilder(int pageSize, PageSortOrder sortOrder) {
        Preconditions.checkArgument(
            pageSize > 0, "pageSize[" + pageSize + "] must be greater than 0."
        );
        Preconditions.checkArgument(
            sortOrder != null, "sortOrder cannot be null."
        );
        this.pageSize = pageSize;
        this.sortOrder = sortOrder;
    }

    public static PageQueryBuilder newBuilder(int pageSize) {
        return new PageQueryBuilder(pageSize, PageSortOrder.ASC);
    }

    public static PageQueryBuilder newBuilder(
        int pageSize, PageSortOrder sortOrder) {
        return new PageQueryBuilder(pageSize, sortOrder);
    }

    public void setStartRow(Object startRow) {
        this.setStartRow(startRow, null);
    }

    public void setStartRow(Object startRow, Boolean inclusiveStartRow) {
        this.startRow = startRow;
        this.inclusiveStartRow = inclusiveStartRow;
    }

    public void setStopRow(Object stopRow) {
        this.setStopRow(stopRow, null);
    }

    public void setRequireRowNum(boolean requireRowNum) {
        this.requireRowNum = requireRowNum;
    }

    public void setStopRow(Object stopRow, Boolean inclusiveStopRow) {
        this.stopRow = stopRow;
        this.inclusiveStopRow = inclusiveStopRow;
    }

    public void setRowKeyRegexp(String rowKeyRegexp) {
        this.rowKeyRegexp = rowKeyRegexp;
    }

    public void setRowKeyPrefix(Object rowKeyPrefix) {
        this.rowKeyPrefix = rowKeyPrefix;
    }

    public void setFamQuaes(Map<String, String[]> famQuaes) {
        this.famQuaes = famQuaes;
    }

    public void setMaxResultSize(int maxResultSize) {
        this.maxResultSize = maxResultSize;
    }

    public void setRowKeyOnly(boolean rowKeyOnly) {
        this.rowKeyOnly = rowKeyOnly;
    }

    // ---------------------------------------------------------getter
    public int getPageSize() {
        return pageSize;
    }

    public int getActualPageSize() {
        return isInclusiveStartRow() ? pageSize : pageSize + 1;
    }

    public Object getStartRow() {
        return startRow;
    }

    public Object getStopRow() {
        return stopRow;
    }

    public Object getRowKeyPrefix() {
        return rowKeyPrefix;
    }

    public String getRowKeyRegexp() {
        return rowKeyRegexp;
    }

    public Map<String, String[]> getFamQuaes() {
        return famQuaes;
    }

    public int getMaxResultSize() {
        return maxResultSize;
    }

    public boolean isRowKeyOnly() {
        return rowKeyOnly;
    }

    public boolean isRequireRowNum() {
        return requireRowNum;
    }

    public PageSortOrder getSortOrder() {
        return ObjectUtils.orElse(sortOrder, PageSortOrder.ASC);
    }

    public boolean isInclusiveStartRow() {
        return ObjectUtils.orElse(inclusiveStartRow, ObjectUtils.isEmpty(startRow));
    }

    public Boolean isInclusiveStopRow() {
        return ObjectUtils.orElse(inclusiveStopRow, true);
    }

    // ----------------------------------------------------------------page start
    public <T> T nextPageStartRow(List<T> results) {
        return pageStartRow(results, true);
    }

    public <T> T previousPageStartRow(List<T> results) {
        return pageStartRow(results, false);
    }

    private <T> T pageStartRow(List<T> results, boolean isNextPage) {
        if (CollectionUtils.isEmpty(results)) {
            return null;
        }

        return results.get(isNextPage ? results.size() - 1 : 0);
    }

}
