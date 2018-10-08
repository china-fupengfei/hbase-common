package code.ponfee.hbase.model;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

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

    private String startRow;
    private String rowKeyRegexp;
    private String rowKeyPrefix;
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

    public static PageQueryBuilder newBuilder(int pageSize, PageSortOrder sortOrder) {
        return new PageQueryBuilder(pageSize, sortOrder);
    }

    public void setStartRow(String startRow) {
        this.startRow = startRow;
    }

    public void setRowKeyRegexp(String rowKeyRegexp) {
        this.rowKeyRegexp = rowKeyRegexp;
    }

    public void setRowKeyPrefix(String rowKeyPrefix) {
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

    public String getStartRow() {
        return startRow;
    }

    public String getRowKeyRegexp() {
        return rowKeyRegexp;
    }

    public String getRowKeyPrefix() {
        return rowKeyPrefix;
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

    public PageSortOrder getSortOrder() {
        return ObjectUtils.orElse(sortOrder, PageSortOrder.ASC);
    }

    public boolean isInclusiveStartRow() {
        return StringUtils.isEmpty(startRow);
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
