package code.ponfee.hbase;

import static code.ponfee.hbase.HbaseHelper.nextStartRowKey;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.annotation.Resource;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import code.ponfee.commons.collect.ByteArrayWrapper;
import code.ponfee.hbase.model.PageQueryBuilder;

/**
 * The Hbase dao common base class
 * 
 * @param <T> the HbaseEntity or HbaseMap
 * @param <R> the hbase rowkey of HbaseEntity or HbaseMap
 * 
 * @author Ponfee
 */
public abstract class HbaseBatchDao<T, R extends Serializable & Comparable<R>> 
    extends HbaseDao<T, R> {

    private static Logger logger = LoggerFactory.getLogger(HbaseBatchDao.class);

    protected @Resource ThreadPoolTaskExecutor taskExecutor;

    public HbaseBatchDao() {
        super();
    }

    // ------------------------------------------------------------------------batch count
    /**
     * Counts for page, include start row
     * 
     * @param query the PageQueryBuilder
     * @return a long is the data count
     */
    public long count(PageQueryBuilder query) {
        AtomicLong total = new AtomicLong(0);
        scrollProcess(query, "Count", rowKeys -> {
            total.addAndGet((long) rowKeys.size());
            rowKeys.clear();
        });
        return total.get();
    }

    // ------------------------------------------------------------------------batch delete
    public boolean delete(PageQueryBuilder query) {
        return delete(query, taskExecutor.getThreadPoolExecutor());
    }

    /**
     * Batch delete for page, inlucde start row
     * 
     * @param query the page query
     * @param threadPoolExecutor the threadPoolExecutor
     * @return {@code true} delete normal
     */
    public boolean delete(PageQueryBuilder query, ThreadPoolExecutor threadPoolExecutor) {
        CompletionService<Boolean> service = new ExecutorCompletionService<>(threadPoolExecutor);
        AtomicInteger round = new AtomicInteger(0);
        scrollProcess(query, "Delete", rowKeys -> {
            service.submit(new AsnycBatchDelete(template, tableName, rowKeys));
            round.incrementAndGet();
        });
        return join(service, round.get(), "delete");
    }

    // ------------------------------------------------------------------------batch copy
    public <E, U extends Serializable & Comparable<U>> boolean copy(
        PageQueryBuilder query, HbaseDao<E, U> target, Consumer<E> callback) {
        return copy(query, target, callback, taskExecutor.getThreadPoolExecutor());
    }

    /**
     * Batch copy for page
     *
     * @param query the page query
     * @param target the target HbaseDao
     * @param callback callback after convert
     * @param threadPoolExecutor the threadPoolExecutor
     * @return {@code true} copy normal
     */
    public <E, U extends Serializable & Comparable<U>> boolean copy(
        PageQueryBuilder query, HbaseDao<E, U> target,
        Consumer<E> callback, ThreadPoolExecutor threadPoolExecutor) {
        AtomicInteger round = new AtomicInteger(0);
        CompletionService<Boolean> service = new ExecutorCompletionService<>(threadPoolExecutor);
        scrollQuery(query, (pageNum, data) -> {
            logger.info("==================Copy at round {}==================", round.get());
            service.submit(new AsnycBatchPut<>(target, target.convert(data, callback)));
            round.incrementAndGet();
        });

        return join(service, round.get(), "copy");
    }

    // ------------------------------------------------------------------------batch put
    public boolean put(List<T> data, int batchSize) {
        return put(data, batchSize, taskExecutor.getThreadPoolExecutor());
    }

    public boolean put(List<T> data, int batchSize, ThreadPoolExecutor threadPoolExecutor) {
        CompletionService<Boolean> service = new ExecutorCompletionService<>(threadPoolExecutor);
        int round = 0;
        for (int from = 0, to, n = data.size(); from < n; from += batchSize) {
            logger.info("==================Put at round {}==================", round);
            to = Math.min(from + batchSize, n);

            service.submit(new AsnycBatchPut<>(this, data.subList(from, to)));
            round++;
        }

        return join(service, round, "put");
    }

    // ------------------------------------------------------------------------query all
    public <E> void scrollQuery(PageQueryBuilder query, BiConsumer<Integer, List<T>> consumer) {
        int pageNum = 0, size;
        query.setRequireRowNum(false);
        List<T> page;
        do {
            page = this.nextPage(query);
            size = page == null ? 0 : page.size();
            if (size > 0) {
                query.setStartRow(super.getRowKey(query.nextPageStartRow(page)), false);
                consumer.accept(++pageNum, page);
            }
            logger.info("==================Scroll Query at round {}==================", pageNum);
        } while (size >= query.getPageSize());
    }

    // ------------------------------------------------------------------------private methods
    /**
     * Scroll process, include start row key
     * 
     * @param query the PageQueryBuilder
     * @param name the ops name
     * @param callback the Consumer callback
     */
    private void scrollProcess(PageQueryBuilder query, String name, 
                               Consumer<List<ByteArrayWrapper>> callback) {
        query.setRowKeyOnly(true);
        List<ByteArrayWrapper> rowKeys;
        int count, round = 0;
        do {
            logger.info("==================" + name + " at round {}==================", round++);
            Scan scan = buildScan(
                query.getStartRow(), query.getStopRow(), 
                query.getActualPageSize(), false, pageScanHook(query)
            );
            //scan.setCaching(2000);
            //scan.setCacheBlocks(false);

            rowKeys = template.find(tableName, scan, results -> {
                List<ByteArrayWrapper> list = new LinkedList<>();
                for (Result result : results) {
                    list.add(ByteArrayWrapper.create(result.getRow()));
                }
                return list;
            });

            // sort list
            rowKeys.sort(Comparator.nullsLast(Comparator.naturalOrder()));

            // data from multiple region server 
            if (rowKeys.size() > query.getPageSize()) {
                rowKeys = rowKeys.subList(0, query.getPageSize());
            }

            count = rowKeys.size();
            if (count > 0) {
                query.setStartRow(nextStartRowKey(rowKeys.get(rowKeys.size() - 1).getArray()), true);
                callback.accept(rowKeys);
            }
        } while (count >= query.getPageSize()); // maybe has next page
    }

    private static boolean join(CompletionService<Boolean> service, 
                                int round, String operation) {
        boolean result = true;
        try {
            while (round > 0) {
                Future<Boolean> future = service.poll();
                if (future != null) {
                    round--;
                    result &= future.get();
                } else {
                    Thread.sleep(31);
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Batch " + operation + " occur error", e);
            result = false;
        }
        return result;
    }

    /**
     * 异步批量删除
     */
    private static final class AsnycBatchDelete implements Callable<Boolean> {
        final HbaseTemplate template;
        final String tableName;
        final List<ByteArrayWrapper> rowKeys;

        AsnycBatchDelete(HbaseTemplate template, String tableName, 
                         List<ByteArrayWrapper> rowKeys) {
            this.template = template;
            this.tableName = tableName;
            this.rowKeys = rowKeys;
        }

        @Override
        public Boolean call() {
            return template.execute(tableName, table -> {
                List<Delete> batch = new ArrayList<>(rowKeys.size());
                for (ByteArrayWrapper array : rowKeys) {
                    batch.add(new Delete(array.getArray()));
                }
                table.batch(batch, new Object[batch.size()]);
                rowKeys.clear();
                return true;
            });
        }
    }

    /**
     * 异步批量增加（修改）数据
     * @param <>
     */
    private static final class AsnycBatchPut<E, U extends Serializable & Comparable<U>> 
        implements Callable<Boolean> {
        final HbaseDao<E, U> dao;
        final List<E> data;

        AsnycBatchPut(HbaseDao<E, U> dao, List<E> data) {
            this.dao = dao;
            this.data = data;
        }

        @Override
        public Boolean call() {
            return dao.put(data);
        }
    }

}