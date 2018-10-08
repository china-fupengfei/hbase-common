package code.ponfee.hbase.test;


import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;

import code.ponfee.hbase.model.PageQueryBuilder;
import code.ponfee.hbase.model.PageSortOrder;
import code.ponfee.hbase.other.BasOrderInfo;
import code.ponfee.hbase.other.BasOrderInfoDao;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:test-hbase.xml" })
public class BasOrderInfoTest {
    
    private static final int PAGE_SIZE = 20;
    private @Resource BasOrderInfoDao hbaseDao;

    @Test
    @Ignore
    public void dropTable() {
        System.out.println(hbaseDao.dropTable());
    }
    
    @Test
    public void createTable() {
        System.out.println(hbaseDao.createTable());
    }

    @Test
    public void descTable() {
        System.out.println(hbaseDao.descTable());
    }

    @Test
    public void put() {
    }

    @Test
    public void batchPut() {
    }

    @Test
    public void get() {
        printJson(hbaseDao.get("4_MEIZU_20160401_S1603290008630_03.21.3211102-T"));
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
        printJson(hbaseDao.range("4_MEIZU_20160401_S1603310002862_03.21.3213102-T", "4_MEIZU_20160401_S1603310004352_03.21.3211104-W"));
    }

    @Test
    public void find() {
        printJson(hbaseDao.find("4_MEIZU_20160401_S1603290008630_03.21.3211102-T", 20));
    }

    @Test
    public void nextPage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        printJson(hbaseDao.nextPage(query));
    }

    @Test
    public void nextPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "name" }));
        List<BasOrderInfo> data = new ArrayList<>();
        int count = 1;
        List<BasOrderInfo> list = (List<BasOrderInfo>) hbaseDao.nextPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.nextPageStartRow(list).getRowKey());
            query.setStartRow((String) query.nextPageStartRow(list).getRowKey());
            list = (List<BasOrderInfo>) hbaseDao.nextPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.getRowKey()));
        System.out.println("======================count: " + count);
        System.out.println("======================" + set.size());
        printJson(set);
    }

    @Test
    public void nextPageAllDESC() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "name" }));
        List<BasOrderInfo> data = new ArrayList<>();
        int count = 1;
        List<BasOrderInfo> list = (List<BasOrderInfo>) hbaseDao.nextPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.nextPageStartRow(list).getRowKey());
            query.setStartRow((String) query.nextPageStartRow(list).getRowKey());
            list = (List<BasOrderInfo>) hbaseDao.nextPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.getRowKey()));
        System.out.println("======================count: " + count);
        System.out.println("======================" + set.size());
        printJson(set);
    }
    
    @Test
    public void previousPage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.setStartRow("name85");
        printJson(hbaseDao.previousPage(query));
    }
    
    @Test
    public void previousPageDesc() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.setStartRow("name85");
        printJson(hbaseDao.previousPage(query));
    }

    @Test
    public void previousPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "name" }));
        query.setStartRow("ponfee2");
        List<BasOrderInfo> data = new ArrayList<>();
        int count = 1;
        List<BasOrderInfo> list = (List<BasOrderInfo>) hbaseDao.previousPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.previousPageStartRow(list).getRowKey());
            query.setStartRow((String) query.previousPageStartRow(list).getRowKey());
            list = (List<BasOrderInfo>) hbaseDao.previousPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.getRowKey()));
        System.out.println("======================count: " + count);
        System.out.println("======================" + set.size());
        printJson(set);
    }

    @Test
    public void previousPageAllDesc() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "name" }));
        query.setStartRow("name10");
        List<BasOrderInfo> data = new ArrayList<>();
        int count = 1;
        List<BasOrderInfo> list = (List<BasOrderInfo>) hbaseDao.previousPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.previousPageStartRow(list).getRowKey());
            query.setStartRow((String) query.previousPageStartRow(list).getRowKey());
            list = (List<BasOrderInfo>) hbaseDao.previousPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.getRowKey()));
        System.out.println("======================count: " + count);
        System.out.println("======================" + set.size());
        printJson(set);
    }

    @Test
    public void page() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "signin_tm" }));
        query.setMaxResultSize(0);
        printJson(hbaseDao.nextPage(query));
    }

    @Test
    public void count() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "age" }));
        query.setMaxResultSize(0);
        printJson("======================" + hbaseDao.count(query));
    }

    // -------------------------------------------------------------------------------
    @Test
    public void prefix() {
        //printJson(extendsHbaseDao1.prefix("name10", "name10", PAGE_SIZE));
        printJson(hbaseDao.prefix("4_MEIZU_20160401_", PAGE_SIZE));
    }

    @Test
    public void regexp() {
        printJson(hbaseDao.regexp("^4_.*_20160101_.*1$", 2));
    }

    @Test
    public void delete() {
        printJson(hbaseDao.get("4_MEIZU_20160401_S1603310004352_03.21.3211104-W"));
        printJson(hbaseDao.delete(new String[] { "4_MEIZU_20160401_S1603310004352_03.21.3211104-W" }));
        printJson(hbaseDao.get("4_MEIZU_20160401_S1603310004352_03.21.3211104-W"));
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
