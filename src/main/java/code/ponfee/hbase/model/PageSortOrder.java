package code.ponfee.hbase.model;

/**
 * Page query Order by
 * 
 * @author Ponfee
 */
public enum PageSortOrder {

    ASC, DESC;

    public static PageSortOrder from(String str) {
        for (PageSortOrder order : PageSortOrder.values()) {
            if (order.name().equalsIgnoreCase(str)) {
                return order;
            }
        }
        return null;
    }
}
