package code.ponfee.hbase.test;

import static code.ponfee.hbase.model.HbaseMap.ROW_KEY_NAME;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

import code.ponfee.commons.util.Dates;
import code.ponfee.hbase.model.PageQueryBuilder;
import code.ponfee.hbase.model.PageSortOrder;
import code.ponfee.hbase.other.ExtendsHbaseMap;
import code.ponfee.hbase.other.ExtendsHbaseMapDao;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:test-hbase.xml" })
public class HbaeDaoMapTest {
    private static final int PAGE_SIZE = 11;
    private @Resource ExtendsHbaseMapDao hbaseDao;

    @Test
    @Ignore
    public void dropTable() {
        System.out.println(hbaseDao.dropTable());
    }

    @Test
    @Ignore
    public void createTable() {
        System.out.println(hbaseDao.createTable());
    }

    @Test
    @Ignore
    public void descTable() {
        System.out.println(hbaseDao.descTable());
    }

    @Test
    @Ignore
    public void batchPut() {
        int count = 200;
        List<ExtendsHbaseMap<Object>> batch = new ArrayList<>();
        Date date = Dates.toDate("20000101", "yyyyMMdd");
        for (int start = 3, i = start; i < count + start; i++) {
            ExtendsHbaseMap<Object> map = new ExtendsHbaseMap<>();
            map.put("age", 1 + ThreadLocalRandom.current().nextInt(60));
            map.put("name", RandomStringUtils.randomAlphanumeric(5));
            map.put("rowKey", Dates.format(Dates.random(date), "yyyyMMddHHmmss"));
            batch.add(map);
        }
        printJson(hbaseDao.put(batch));
    }

    @Test
    public void get() {
        printJson(hbaseDao.get("20000201211046"));
    }

    @Test
    public void first() {
        printJson(hbaseDao.first());
    }
    
    @Test
    public void last() {
        printJson(hbaseDao.last());
    }

    @Test
    public void range() {
        printJson(hbaseDao.range("20000201211046", "20060201211046"));
    }

    @Test
    public void find() {
        printJson(hbaseDao.find("20041014150203", "20050828085930", 20));
        printJson(hbaseDao.find("20041014150203", "20050828085930", 2));
        printJson(hbaseDao.find("20050828085930", "20041014150203", 20, true));
        printJson(hbaseDao.find("20050828085930", "20041014150203", 2, true));
    }

    @Test
    public void findAll() {
        List<ExtendsHbaseMap<Object>> list = (List<ExtendsHbaseMap<Object>>) hbaseDao.range(null, null);
        System.out.println("======================" + list.size());
        printJson(list);
    }

    @Test
    public void nextPage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.requireRowNum(false);
        //query.rowKeyOnly();
        printJson(hbaseDao.nextPage(query));
    }

    @Test
    public void nextPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(111, PageSortOrder.ASC);
        query.startRowKey("00000000");
        //query.setRowKeyPrefix("fu_ponfee_2009");
        //Set<String> set = new TreeSet<>();
        Set<String> set = new LinkedHashSet<>();
        hbaseDao.scrollQuery(query, (pageNum, data)->{
            System.err.println("======================pageNum: " + pageNum);
            printJson(data);
            data.stream().forEach(m -> set.add((String)m.getRowKey()));
        });
        System.err.println("======================" + set.size());
        printJson(set);
    }

    @Test
    public void previousPage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.startRowKey("20050828085930");
        printJson(hbaseDao.previousPage(query));
    }
    
    @Test
    public void previousPageDesc() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.startRowKey("20050828085930");
        printJson(hbaseDao.previousPage(query));
    }

    @Test
    public void previousPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(111);
        query.addColumns("cf1", "name");
        query.startRowKey("20181004162958");
        List<ExtendsHbaseMap<Object>> data = new ArrayList<>();
        int count = 1;
        List<ExtendsHbaseMap<Object>> list = (List<ExtendsHbaseMap<Object>>) hbaseDao.previousPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.pageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.previousPageStartRow(list).get(ROW_KEY_NAME));
            query.startRowKey((String) query.previousPageStartRow(list).get(ROW_KEY_NAME));
            list = (List<ExtendsHbaseMap<Object>>) hbaseDao.previousPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.get(ROW_KEY_NAME)));
        System.out.println("======================count: " + count);
        System.out.println("======================" + set.size());
        printJson(set);
    }

    @Test
    public void page() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(1);
        printJson(hbaseDao.nextPage(query));
    }

    @Test
    public void count() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(111);
        printJson("======================" + hbaseDao.count(query));
    }

    // -------------------------------------------------------------------------------
    @Test
    public void prefix() {
        //printJson(extendsHbaseDao1.prefix("name10", "name10", PAGE_SIZE));
        printJson(hbaseDao.prefix("2018", PAGE_SIZE));
    }

    @Test
    public void regexp() {
        //printJson(extendsHbaseDao1.regexp("^name.*1$", "name10", PAGE_SIZE));
        printJson(hbaseDao.regexp("^20[0-1]{1}8.*1$", 20));
    }

    @Test
    @Ignore
    public void delete() {
        printJson(hbaseDao.delete(Lists.newArrayList("20171231050359","20170922213037" )));
    }

    private static void printJson(Object obj) {
        try {
            Thread.sleep(100);
            System.err.println(JSONObject.toJSONString(obj));
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
