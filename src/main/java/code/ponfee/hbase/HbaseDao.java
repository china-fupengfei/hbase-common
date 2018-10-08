package code.ponfee.hbase;

import static code.ponfee.hbase.model.HbaseMap.ROW_KEY_NAME;
import static code.ponfee.hbase.model.HbaseMap.ROW_NUM_NAME;
import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.substringBefore;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.RandomAccess;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;

import code.ponfee.commons.cache.DateProvider;
import code.ponfee.commons.math.Numbers;
import code.ponfee.commons.reflect.ClassUtils;
import code.ponfee.commons.reflect.Fields;
import code.ponfee.commons.reflect.GenericUtils;
import code.ponfee.commons.util.Holder;
import code.ponfee.commons.util.ObjectUtils;
import code.ponfee.commons.util.Strings;
import code.ponfee.hbase.annotation.HbaseField;
import code.ponfee.hbase.annotation.HbaseTable;
import code.ponfee.hbase.model.HbaseEntity;
import code.ponfee.hbase.model.HbaseMap;
import code.ponfee.hbase.model.PageQueryBuilder;
import code.ponfee.hbase.model.PageSortOrder;

/**
 * The Hbase dao common base class
 * 
 * @author Ponfee
 */
public abstract class HbaseDao<T> {

    private static Logger logger = LoggerFactory.getLogger(HbaseDao.class);

    private static final int REVERSE_THRESHOLD = 18;
    //private static final DateProvider DATE_PROV = DateProvider.CURRENT;
    private static final DateProvider PROVIDER = DateProvider.LATEST;
    private static final byte[] EMPTY_BYTE_ARRAY = {};

    protected final Class<T> type;
    protected final ImmutableBiMap<String, Field> fieldMap;
    protected final String tableName;
    protected final String globalFamily;
    protected final List<byte[]> definedFamilies;

    private @Resource HbaseTemplate template;

    public HbaseDao() {
        Class<?> clazz = this.getClass();
        this.type = GenericUtils.getActualTypeArgument(clazz);
        if (   !HbaseEntity.class.isAssignableFrom(this.type)
            && !HbaseMap.class.isAssignableFrom(this.type)
        ) {
            throw new UnsupportedOperationException(
                "The class generic type must be HbaseEntity or HbaseMap"
            );
        }

        try {
            type.getConstructor();
        } catch (NoSuchMethodException | SecurityException e) {
            throw new UnsupportedOperationException(
                "The class " + clazz.getSimpleName() + " default constructor not found."
            );
        }

        this.fieldMap = ImmutableBiMap.<String, Field> builder().putAll(
            ClassUtils.listFields(this.type).stream().collect(
                Collectors.toMap(f -> {
                    HbaseField hf = f.getAnnotation(HbaseField.class);
                    return (hf == null || isEmpty(hf.qualifier()))
                           ? LOWER_CAMEL.to(LOWER_UNDERSCORE, f.getName()) 
                           : hf.qualifier();
                }, Function.identity())
            )
        ).build();

        // table name
        HbaseTable ht = this.type.getDeclaredAnnotation(HbaseTable.class);
        String tableName = (ht != null && isNotEmpty(ht.tableName())) 
                           ? ht.tableName()
                           : LOWER_CAMEL.to(LOWER_UNDERSCORE, clazz.getSimpleName());
        this.tableName = buildTableName(ht.namespace(), tableName);

        // global family
        this.globalFamily = ht != null ? ht.family() : null;

        // entity Families
        ImmutableList.Builder<byte[]> builder = new ImmutableList.Builder<>();
        if (isNotEmpty(this.globalFamily)) {
            builder.add(toBytes(this.globalFamily));
        }
        this.fieldMap.values().stream().forEach(field -> {
            HbaseField hf = field.getDeclaredAnnotation(HbaseField.class);
            if (hf != null && isNotEmpty(hf.family())) {
                builder.add(toBytes(hf.family()));
            }
        });
        definedFamilies = builder.build();
    }

    // ------------------------------------------------------------------config and connection
    protected final Configuration getConfig() {
        return template.getConfiguration();
    }

    protected final Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(getConfig());
    }

    protected final Table getTable(Connection conn, String tableName) throws IOException {
        return conn.getTable(TableName.valueOf(tableName));
    }

    protected final void closeConnection(Connection conn) {
        if (conn != null) try {
            conn.close();
        } catch (Exception e) {
            logger.error("Close hbase connection occur error.", e);
        }
    }

    protected final void closeTable(Table table) {
        if (table != null) try {
            table.close();
        } catch (Exception e) {
            logger.error("Close hbase table occur error.", e);
        }
    }

    // ------------------------------------------------------------------admin operations
    public boolean createTable() {
        // this.tableName include namespace
        return createTable(null, this.tableName, definedFamilies);
    }

    public boolean createTable(String tableName, String[] colFamilies) {
        return createTable(null, tableName, colFamilies);
    }

    public boolean createTable(String namespace, String tableName, String[] colFamilies) {
        List<byte[]> families = Stream.of(colFamilies).map(HbaseDao::toBytes)
                                      .collect(Collectors.toList());
        return createTable(namespace, tableName, families);
    }

    /**
     * Returns create hbase table result, if return {@code true}
     * means create success
     * 
     * @param namespace   the hbase namespace, if null then use hbase default
     * @param tableName   the hbase table name
     * @param colFamilies the hbase column families
     * @return if create success then return {@code true}
     */
    public boolean createTable(String namespace, String tableName, 
                               @Nonnull List<byte[]> colFamilies) {
        Preconditions.checkArgument(isNotEmpty(tableName));
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(colFamilies));
        try (Connection conn = getConnection();
             Admin admin = conn.getAdmin()
        ) {
            String namespace0 = isEmpty(namespace) && tableName.indexOf(':') != -1 
                                ? substringBefore(tableName, ":") : namespace;
            if (   isNotBlank(namespace0) 
                && !Stream.of(admin.listNamespaceDescriptors())
                          .anyMatch(nd -> nd.getName().equals(namespace0))
            ) {
                // 创建表空间
                admin.createNamespace(NamespaceDescriptor.create(namespace0).build());
            }
            TableName table = TableName.valueOf(buildTableName(namespace, tableName));
            if (admin.tableExists(table)) {
                logger.warn("Hbase table {}:{} exists.", namespace, tableName);
                return false;
            }
            HTableDescriptor tableDesc = new HTableDescriptor(table);
            //tableDesc.setDurability(Durability.USE_DEFAULT); // 设置为默认的Write-Ahead-Log级别
            colFamilies.forEach(family -> {
                HColumnDescriptor hcd = new HColumnDescriptor(family);
                //hcd.setTimeToLive(5184000); // 设置数据保存的最长时间
                //hcd.setMaxVersions(1); // 设置数据保存的最大版本数
                //hcd.setInMemory(true); // 设置数据保存在内存中以提高响应速度
                tableDesc.addFamily(hcd);
            });
            admin.createTable(tableDesc);
            return true;
        } catch (IOException e) {
            logger.error("Create hbase table {}:{} occur error.", namespace, tableName, e);
            return false;
        }
    }

    public List<String> descTable() {
        // this.tableName include namespace
        return descTable(null, this.tableName);
    }

    public List<String> descTable(String tableName) {
        return descTable(null, tableName);
    }

    /**
     * Returns the hbase table column family info
     * 
     * @param namespace the hbase namespace, if null then use hbase default 
     * @param tableName the hbase table name
     * @return a list string of column families info
     */
    public List<String> descTable(String namespace, String tableName) {
        tableName = buildTableName(namespace, tableName);
        return template.execute(tableName, table -> {
            HTableDescriptor desc = table.getTableDescriptor();
            List<String> result = new ArrayList<>();
            for (HColumnDescriptor hcd : desc.getColumnFamilies()) {
                result.add(hcd.toString());
            }
            return result;
        });
    }

    public boolean dropTable() {
        // this.tableName include namespace
        return dropTable(null, this.tableName);
    }

    public boolean dropTable(String tableName) {
        return dropTable(null, tableName);
    }

    /**
     * Returns drop hbase table result
     * 
     * @param namespace the hbase namespace, if null then use hbase default 
     * @param tableName the hbase table name
     * @return if {@code true} means drop success
     */
    public boolean dropTable(String namespace, String tableName) {
        try (Connection conn = getConnection();
             Admin admin = conn.getAdmin()
        ) {
            TableName table = TableName.valueOf(buildTableName(namespace, tableName));
            if (!admin.tableExists(table)) {
                logger.warn("Hbase table {}:{} not exists.", namespace, tableName);
                return false;
            }
            admin.disableTable(table);
            admin.deleteTable(table);
            return true;
        } catch (IOException e) {
            logger.error("Drop hbase table {}:{} occur error.", namespace, tableName, e);
            return false;
        }
    }

    // ------------------------------------------------------------------get by row key
    public T get(String rowKey) {
        return get(rowKey, globalFamily, null);
    }

    public T get(String rowKey, String familyName) {
        return get(rowKey, familyName, null);
    }

    /**
     * Returns a hbase row data spec rowKey
     * 
     * @param rowKey the hbase row key
     * @param familyName the hbase column family name 
     * @param qualifier then hbase column qualifier
     * @return a hbase row data
     */
    public T get(String rowKey, String familyName, String qualifier) {
        return template.get(tableName, rowKey, familyName, 
                            qualifier, rowMapper(rowKey, true));
    }

    // ------------------------------------------------------------------find data list
    public List<T> find(String startRow, int pageSize) {
        return find(startRow, null, pageSize, false);
    }

    public List<T> find(String startRow, String stopRow, int pageSize) {
        return find(startRow, stopRow, pageSize, false);
    }

    /**
     * Returns hbase row data list, where condition is 
     * rowkey between startRow and stopRow,
     * if pageSize < 1 then return all condition data 
     * else maximum returns pageSize data list
     * 
     * @param startRow  start row key
     * @param stopRow   stop row key
     * @param pageSize page size
     * @param reversed the reversed order, if {@code true} then startRow > stopRow 
     *                 eg: <code>find("name94", "name89", 20, true)</code>
     * @return hbase row data list
     */
    public List<T> find(String startRow, String stopRow, int pageSize, boolean reversed) {
        return find(startRow, stopRow, pageSize, reversed, scan -> {
            addDefinedFamilies(scan);
            // include stop row
            if (isNotEmpty(stopRow)) {
                scan.setFilter(new InclusiveStopFilter(toBytes(stopRow)));
            }
        }, true);
    }

    // ------------------------------------------------------------------rang with start and stop row key
    public List<T> range(String startRow, String stopRow) {
        return find(startRow, stopRow, 0, false);
    }

    // ------------------------------------------------------------------find by row key prefix
    public List<T> prefix(String rowKeyPrefix) {
        return prefix(rowKeyPrefix, null, 0);
    }

    public List<T> prefix(String rowKeyPrefix, String startRow) {
        return prefix(rowKeyPrefix, startRow, 0);
    }

    public List<T> prefix(String rowKeyPrefix, int pageSize) {
        return prefix(rowKeyPrefix, null, pageSize);
    }

    /**
     * Returns hbase row data list, where condition is rowkey begin startRow,
     * and rowkey must prefix in rowKeyPrefix
     * if pageSize < 1 then return all condition data 
     * else maximum returns pageSize data list
     * 
     * @param rowKeyPrefix the row key prefix
     * @param startRow  the start row
     * @param pageSize  the page size
     * @return hbase row data list
     */
    public List<T> prefix(String rowKeyPrefix, String startRow, int pageSize) {
        return find(startRow, null, pageSize, false, scan -> {
            addDefinedFamilies(scan);
            scan.setFilter(new PrefixFilter(toBytes(rowKeyPrefix)));
        }, true);
    }

    // ------------------------------------------------------------------find by row key regexp
    public List<T> regexp(String rowKeyRegexp) {
        return regexp(rowKeyRegexp, null, 0);
    }

    public List<T> regexp(String rowKeyRegexp, String startRow) {
        return regexp(rowKeyRegexp, startRow, 0);
    }

    public List<T> regexp(String rowKeyRegexp, int pageSize) {
        return regexp(rowKeyRegexp, null, pageSize);
    }

    /**
     * Returns hbase row data list, where condition is rowkey begin startRow,
     * and rowkey with match rowKeyRegexp pattern
     * if pageSize < 1 then return all condition data 
     * else maximum returns pageSize data list
     * 
     * @param rowKeyRegexp the row key regexp pattern, eg: "^name.*1$"
     * @param startRow  the start row
     * @param pageSize  the page size
     * @return hbase row data list
     */
    public List<T> regexp(String rowKeyRegexp, String startRow, int pageSize) {
        return find(startRow, null, pageSize, false, scan -> {
            addDefinedFamilies(scan);
            RegexStringComparator regexp = new RegexStringComparator(rowKeyRegexp);
            scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, regexp));
        }, true);
    }

    // ------------------------------------------------------------------find for page
    public List<T> nextPage(PageQueryBuilder query) {
        return page(query, true, query.getSortOrder() != PageSortOrder.ASC);
    }

    public List<T> previousPage(PageQueryBuilder query) {
        return page(query, false, query.getSortOrder() == PageSortOrder.ASC);
    }

    public long count(PageQueryBuilder query) {
        query.setRowKeyOnly(true);
        return count(query, 0);
    }

    // ------------------------------------------------------------------get the last|first row
    public T first() {
        List<T> result = find(null, null, 1, false);
        return CollectionUtils.isEmpty(result) ? null : result.get(0);
    }

    public T last() {
        List<T> result = find(null, null, 1, true);
        return CollectionUtils.isEmpty(result) ? null : result.get(0);
    }

    // ------------------------------------------------------------------put value into hbase
    public boolean put(String tableName, String rowKey, String familyName,
                       String qualifier, String value) {
        return template.execute(tableName, table -> {
            Put put = new Put(toBytes(rowKey));
            put.addColumn(toBytes(familyName), toBytes(qualifier), PROVIDER.now(), toBytes(value));
            table.put(put);
            return true;
        });
    }

    public boolean put(String tableName, String rowKey, String familyName,
                       String[] qualifiers, Object[] values) {
        return template.execute(tableName, table -> {
            Put put = new Put(toBytes(rowKey));
            byte[] family = toBytes(familyName);
            long now = PROVIDER.now();
            for (int n = qualifiers.length, i = 0; i < n; i++) {
                put.addColumn(family, toBytes(qualifiers[i]), now, toBytes(values[i]));
            }
            table.put(put);
            return true;
        });
    }

    // ------------------------------------------------------------------put one row data into hbase
    public boolean put(String tableName, String rowKey, 
                       String familyName, Map<String, Object> data) {
        Preconditions.checkArgument(isNotEmpty(rowKey));
        return template.execute(tableName, table -> {
            Put put = new Put(toBytes(rowKey));
            byte[] family = toBytes(familyName);
            long now = PROVIDER.now();
            data.entrySet().stream().filter(
                e -> isNotEmpty(e.getKey()) && !ROW_KEY_NAME.equals(e.getKey())
            ).forEach(entry -> {
                put.addColumn(family, toBytes(entry.getKey()), now, toBytes(entry.getValue()));
            });
            table.put(put);
            return true;
        });
    }

    // ------------------------------------------------------------------batch put row data into hbase
    @SuppressWarnings("unchecked")
    public boolean put(String tableName, String familyName, Map<String, Object> data) {
        return put(tableName, familyName, new Map[] { data });
    }

    @SuppressWarnings("unchecked")
    public boolean put(String tableName, String familyName, Map<String, Object>... array) {
        return template.execute(tableName, table -> {
            byte[] family = toBytes(familyName);
            long now = PROVIDER.now();
            List<Put> batch = new ArrayList<>(array.length);
            for (Map<String, Object> data : array) {
                Put put = new Put(toBytes((String) data.get(ROW_KEY_NAME)));
                data.entrySet().stream().filter(
                    e -> isNotEmpty(e.getKey()) && !ROW_KEY_NAME.equals(e.getKey())
                ).forEach(entry -> {
                    put.addColumn(family, toBytes(entry.getKey()), now, toBytes(entry.getValue()));
                });
                batch.add(put);
            }
            //table.put(batch);
            table.batch(batch, new Object[batch.size()]);
            return true;
        });
    }

    // ------------------------------------------------------------------put batch data into hbase
    @SuppressWarnings("unchecked")
    public <V> boolean put(T... data) {
        return put(null, data);
    }

    @SuppressWarnings("unchecked")
    public <V> boolean put(String familyName, T... data) {
        if (ArrayUtils.isEmpty(data)) {
            return false;
        }

        return template.execute(tableName, table -> {
            List<Put> batch = new ArrayList<>(data.length);
            long now = PROVIDER.now();
            byte[] fam = toBytes(familyName);
            for (T obj : data) {
                if (obj instanceof HbaseEntity) {
                    HbaseEntity entity = (HbaseEntity) obj;
                    Put put = new Put(toBytes(entity.getRowKey()));
                    this.fieldMap.values().stream().forEach(field -> {
                        HbaseField hf = field.getDeclaredAnnotation(HbaseField.class);
                        if (hf != null && hf.ignore()) { // 忽略的字段
                            return;
                        }
                        byte[] family = getFamily(fam, hf, field);
                        byte[] qualifier = getQualifier(field);
                        byte[] value = toBytes(getValue(entity, field, hf));
                        put.addColumn(family, qualifier, now, value);
                    });
                    if (!put.isEmpty()) {
                        batch.add(put);
                    }
                } else if (obj instanceof HbaseMap) {
                    HbaseMap<V> map = (HbaseMap<V>) obj;
                    byte[] family = getFamily(fam, null, null);
                    if (family == null) {
                        throw new IllegalArgumentException("Family cannot be null.");
                    }
                    Put put = new Put(toBytes(map.get(ROW_KEY_NAME)));
                    map.entrySet().stream().filter(
                        e -> isNotEmpty(e.getKey()) && !ROW_KEY_NAME.equals(e.getKey())
                    ).forEach(entry -> {
                        put.addColumn(family, toBytes(entry.getKey()), now, toBytes(entry.getValue()));
                    });
                    if (!put.isEmpty()) {
                        batch.add(put);
                    }
                } else {
                    throw new UnsupportedOperationException("Unsupported type: " + type.getCanonicalName());
                }
            }

            if (batch.isEmpty()) {
                logger.warn("Empty batch put.");
                return false;
            } else {
                table.batch(batch, new Object[batch.size()]);
                return true;
            }
        });
    }

    // ------------------------------------------------------------------delete data from hbase spec rowkey
    public boolean delete(String[] rowKeys) {
        return delete(tableName, rowKeys, null);
    }

    public boolean deleteFamily(String[] rowKeys) {
        return delete(tableName, rowKeys, definedFamilies);
    }

    public boolean delete(String tableName, String[] rowKeys) {
        return delete(tableName, rowKeys, null);
    }

    public boolean delete(String tableName, String[] rowKeys, List<byte[]> families) {
        return template.execute(tableName, table -> {
            List<Delete> batch = new ArrayList<>(rowKeys.length);
            for (String rowKey : rowKeys) {
                Delete delete = new Delete(Bytes.toBytes(rowKey));
                if (families != null) {
                    families.stream().forEach(family -> delete.addFamily(family));
                }
                batch.add(delete);
            }
            table.batch(batch, new Object[batch.size()]);
            return true;
        });
    }

    // ------------------------------------------------------------------private methods
    private List<T> find(String startRow, String stopRow, int pageSize, boolean reversed, 
                         ScanHook scanHook, boolean inclusiveStartRow) {
        Scan scan = buildScan(startRow, stopRow, pageSize, reversed, scanHook);
        List<T> result = template.find(tableName, scan, rowMapper(null, inclusiveStartRow));
        if (CollectionUtils.isNotEmpty(result) && !inclusiveStartRow) {
            result = result.subList(1, result.size());
        }
        return result;
    }

    private List<T> page(PageQueryBuilder query, boolean isNextPage, boolean reversed) {
        List<T> result = find(query.getStartRow(), null, query.getActualPageSize(), 
                              reversed, pageScanHook(query), query.isInclusiveStartRow());

        if (!isNextPage && CollectionUtils.isNotEmpty(result)) {
            //Collections.reverse(result);
            int size = result.size();
            if (size < REVERSE_THRESHOLD || result instanceof RandomAccess) {
                for (int i = 0, mid = size >> 1, j = size - 1; i < mid; i++, j--) {
                    swap(result, i, j);
                }
            } else {
                ListIterator<T> fwd = result.listIterator();
                ListIterator<T> rev = result.listIterator(size);
                for (int i = 0, mid = result.size() >> 1; i < mid; i++) {
                    T next = fwd.next();
                    T prev = rev.previous();
                    swapRowNum(next, prev);
                    fwd.set(prev);
                    rev.set(next);
                }
            }
        }
        return result;
    }

    private long count(PageQueryBuilder query, long total) {
        Scan scan = buildScan(
            query.getStartRow(), null, 
            query.getActualPageSize(), 
            false, pageScanHook(query)
        );
        scan.setCaching(2000);
        scan.setCacheBlocks(false);

        // others
        int count = template.find(tableName, scan, results -> {
            int number = 0;
            Result last = null;
            for (Result result : results) {
                last = result;
                number++;
            }
            if (number > 0 && !query.isInclusiveStartRow()) {
                number -= 1;
            }
            if (last != null && !last.isEmpty() && number == query.getPageSize()) {
                query.setStartRow(Bytes.toString(CellUtil.cloneRow(last.listCells().get(0))));
            }
            return number;
        });

        total += count;
        return count < query.getPageSize() ? total : count(query, total);
    }

    private Scan buildScan(String startRow, String stopRow, int pageSize,
                           boolean reversed, ScanHook scanHook) {
        Scan scan = new Scan();
        scan.setReversed(reversed);
        if (isNotEmpty(startRow)) {
            scan.setStartRow(toBytes(startRow));
        }

        scanHook.hook(scan);

        Filter filter = scan.getFilter();
        if (isNotEmpty(stopRow) && !containsFilter(InclusiveStopFilter.class, filter)) {
            scan.setStopRow(toBytes(stopRow));
        }

        if (pageSize > 0) {
            if (filter == null) {
                filter = new PageFilter(pageSize);
            } else {
                if (!(filter instanceof FilterList)) {
                    filter = new FilterList(Operator.MUST_PASS_ALL, filter);
                }
                ((FilterList) filter).addFilter(new PageFilter(pageSize));
            }
        }
        scan.setFilter(filter);
        return scan;
    }

    private ScanHook pageScanHook(PageQueryBuilder query) {
        return scan -> {
            // filters
            // 提取rowkey以01结尾数据： new RowFilter(CompareOp.EQUAL, new RegexStringComparator(".*01$"));
            // 提取rowkey以包含201407的数据：new RowFilter(CompareOp.EQUAL, new SubstringComparator("201407"));
            // 提取rowkey以123开头的数据：new RowFilter(CompareOp.EQUAL, new BinaryPrefixComparator("123".getBytes()));
            FilterList filters = new FilterList(Operator.MUST_PASS_ALL);
            if (isNotEmpty(query.getRowKeyPrefix())) {
                filters.addFilter(new PrefixFilter(toBytes(query.getRowKeyPrefix())));
            }
            if (isNotEmpty(query.getRowKeyRegexp())) {
                RegexStringComparator regexp = new RegexStringComparator(query.getRowKeyRegexp());
                filters.addFilter(new RowFilter(CompareOp.EQUAL, regexp));
            }

            // query column
            if (query.isRowKeyOnly()) {
                filters.addFilter(new FirstKeyOnlyFilter());
                //filters.addFilter(new KeyOnlyFilter());
            } else if (MapUtils.isNotEmpty(query.getFamQuaes())) {
                query.getFamQuaes().entrySet().forEach(entry -> {
                    byte[] family = toBytes(entry.getKey());
                    String[] qualifies = entry.getValue();
                    if (ArrayUtils.isEmpty(qualifies)) {
                        scan.addFamily(family);
                    } else {
                        Stream.of(qualifies).forEach(q -> scan.addColumn(family, toBytes(q)));
                    }
                });
            } else {
                addDefinedFamilies(scan);
            }

            scan.setFilter(filters);

            // others
            if (query.getMaxResultSize() > -1) {
                scan.setMaxResultSize(query.getMaxResultSize());
            }
        };
    }

    private void addDefinedFamilies(Scan scan) {
        definedFamilies.stream().forEach(family -> scan.addFamily(family));
    }

    @SuppressWarnings("unchecked")
    private RowMapper<T> rowMapper(String rowKey, boolean inclusiveStartRow) {
        return (result, rowNum) -> {
            if (result.isEmpty() || (rowNum == 0 && !inclusiveStartRow)) {
                return null;
            }

            int rowNum0 = inclusiveStartRow ? rowNum : rowNum - 1;
            Holder<Boolean> isSetRowKey = Holder.of(false);
            Object model = type.newInstance();
            if (HbaseEntity.class.isAssignableFrom(type)) {
                HbaseEntity entity = (HbaseEntity) model;
                Fields.put(entity, ROW_NUM_NAME, rowNum0);
                if (rowKey != null) {
                    Fields.put(entity, ROW_KEY_NAME, rowKey);
                    isSetRowKey.set(true);
                }
                result.listCells().stream().forEach(cell -> {
                    if (!isSetRowKey.get()) {
                        Fields.put(entity, ROW_KEY_NAME, Bytes.toString(CellUtil.cloneRow(cell)));
                        isSetRowKey.set(true);
                    }
                    // CellUtil.cloneFamily(cell), cell.getTimestamp(), cell.getSequenceId()
                    setValue(entity, Bytes.toString(CellUtil.cloneQualifier(cell)), CellUtil.cloneValue(cell));
                });
                return (T) entity;
            } else if (HbaseMap.class.isAssignableFrom(type)) {
                Map<String, Object> map = (HbaseMap<Object>) model;
                map.put(ROW_NUM_NAME, rowNum0);
                if (rowKey != null) {
                    map.put(ROW_KEY_NAME, rowKey);
                    isSetRowKey.set(true);
                }
                result.listCells().stream().forEach(cell -> {
                    if (!isSetRowKey.get()) {
                        map.put(ROW_KEY_NAME, Bytes.toString(CellUtil.cloneRow(cell)));
                        isSetRowKey.set(true);
                    }
                    map.put(Bytes.toString(CellUtil.cloneQualifier(cell)), 
                            Bytes.toString(CellUtil.cloneValue(cell)));
                });
                return (T) map;
            } else {
                throw new UnsupportedOperationException("Unsupported type: " + type.getCanonicalName());
            }
        };
    }

    /**
     * Returns the family name for hbase
     * 
     * @param family     spec family name in methods
     * @param hf         the filed annotation's family name
     * @param field    the field name
     * @return a family name of hbase
     */
    private byte[] getFamily(byte[] family, HbaseField hf, Field field) {
        if (ArrayUtils.isNotEmpty(family)) { // first spec level family
            return family;
        }
        if (hf != null && isNotEmpty(hf.family())) { // second field level family
            return toBytes(hf.family());
        }
        if (!definedFamilies.isEmpty()) { // third global level family
            return definedFamilies.get(0);
        }
        if (field != null) { // least filed name level family
            return toBytes(LOWER_CAMEL.to(LOWER_UNDERSCORE, field.getName()));
        }
        return null;
    }

    private byte[] getQualifier(Field field) {
        String qualifier = this.fieldMap.inverse().get(field);
        if (qualifier == null) {
            qualifier = LOWER_CAMEL.to(LOWER_UNDERSCORE, field.getName());
        }
        return toBytes(qualifier);
    }

    private void setValue(Object target, String qualifier, byte[] value) {
        if (value == null || value.length == 0) {
            return;
        }

        Field field = fieldMap.get(qualifier);
        if (field == null) {
            return; // not exists qualifier field name
        }

        HbaseField hf = field.getDeclaredAnnotation(HbaseField.class);
        if (hf != null && hf.serial()) {
            Fields.put(target, field, ObjectUtils.deserialize(value, field.getType()));
        } else if (   hf != null && ArrayUtils.isNotEmpty(hf.format()) 
                   && Date.class.isAssignableFrom(field.getType())
        ) {
            Date date;
            String str = Bytes.toString(value);
            if (hf.format().length == 1 
                && HbaseField.FORMAT_TIMESTAMP.equalsIgnoreCase(hf.format()[0])) {
                date = new Date(Numbers.toLong(str));
            } else {
                try {
                    date = DateUtils.parseDate(str, hf.format());
                } catch (ParseException e) {
                    throw new RuntimeException("Invalid date format: " + str);
                }
            }
            Fields.put(target, field, date);
        } else {
            String str = Bytes.toString(value);
            if (field.getType().isPrimitive() && Strings.isEmpty(str)) {
                return;
            }
            try {
                Fields.put(target, field, ObjectUtils.convert(str, field.getType()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static <E extends Filter> boolean containsFilter(Class<E> type, Filter filter) {
        if (filter == null) {
            return false;
        }
        if (!(filter instanceof FilterList)) {
            return type.isInstance(filter);
        }
        return ((FilterList) filter).getFilters().stream()
                                    .anyMatch(f -> type.isInstance(f));
    }

    private static byte[] toBytes(String str) {
        if (str == null) {
            return null;
        }
        if (isEmpty(str)) {
            return EMPTY_BYTE_ARRAY;
        }
        return Bytes.toBytes(str);
    }

    private static byte[] toBytes(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        if (obj instanceof Byte[]) {
            ArrayUtils.toPrimitive((Byte[]) obj);
        }
        if (obj instanceof InputStream) {
            try (InputStream input = (InputStream) obj) {
                return IOUtils.toByteArray(input);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        String str;
        if (isEmpty(str = obj.toString())) {
            return EMPTY_BYTE_ARRAY;
        }
        return Bytes.toBytes(str);
    }

    private static byte[] getValue(Object target, Field field, HbaseField hf) {
        Object value = Fields.get(target, field);
        if (value == null) {
            return null;
        }
        if (hf != null && hf.serial()) {
            return ObjectUtils.serialize(value, field.getType());
        }
        if (hf == null || ArrayUtils.isEmpty(hf.format())) {
            return toBytes(value.toString());
        }

        // HbaseField meta
        if (Date.class.isInstance(value)) {
            if (hf.format().length == 1
                && HbaseField.FORMAT_TIMESTAMP.equalsIgnoreCase(hf.format()[0])) {
                return toBytes(Long.toString(((Date) value).getTime()));
            } else {
                return toBytes(FastDateFormat.getInstance(hf.format()[0]).format(value));
            }
        }
        return toBytes(value.toString());
    }

    private static String buildTableName(String namespace, String tableName) {
        return isNotBlank(namespace) 
               ? namespace + ":" + tableName : tableName;
    }

    private static <T> void swap(List<T> list, int i, int j) {
        T t1 = list.get(i);
        swapRowNum(t1, list.get(j));
        list.set(i, list.set(j, t1));
    }

    @SuppressWarnings("unchecked")
    private static <T> void swapRowNum(T t1, T t2) {
        int rowNum1;
        if (t1 instanceof HbaseEntity) {
            HbaseEntity h1 = (HbaseEntity) t1;
            HbaseEntity h2 = (HbaseEntity) t2;
            rowNum1 = h1.getRowNum();
            h1.setRowNum(h2.getRowNum());
            h2.setRowNum(rowNum1);
        } else {
            Map<String, Object> m1 = (Map<String, Object>) t1;
            Map<String, Object> m2 = (Map<String, Object>) t2;
            rowNum1 = (int) m1.get(ROW_NUM_NAME);
            m1.put(ROW_NUM_NAME, m2.get(ROW_NUM_NAME));
            m2.put(ROW_NUM_NAME, rowNum1);
        }
    }

    /**
     * Hbase scan hook
     */
    @FunctionalInterface
    private static interface ScanHook {
        void hook(Scan scan);
    }

}
