package com.sf.sids.hbase.test;

import static com.sf.sids.hbase.bean.HbaseMap.ROW_KEY_NAME;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.sf.sids.hbase.bean.PageQueryBuilder;
import com.sf.sids.hbase.bean.PageSortOrder;
import com.sf.sids.hbase.other.ExtendsHbaseMap;
import com.sf.sids.hbase.other.ExtendsHbaseMapDao;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:test-hbase.xml" })
public class HbaeDaoMapTest {
    private static final int PAGE_SIZE = 20;
    private @Resource ExtendsHbaseMapDao hbaseDao;

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
        ExtendsHbaseMap<Object> map = new ExtendsHbaseMap<>();
        map.put("name", "ponfee");
        map.put("age", 3);
        map.put("time", new Date());
        map.put("rowKey", "ponfee");
        printJson(hbaseDao.put(map));
        
        map = new ExtendsHbaseMap<>();
        map.put("name", "ponfee1");
        map.put("age", 3);
        map.put("time", new Date());
        map.put("rowKey", "ponfee1");
        printJson(hbaseDao.put(map));
        
        map = new ExtendsHbaseMap<>();
        map.put("name", "ponfee2");
        map.put("age", 3);
        map.put("time", new Date());
        map.put("rowKey", "ponfee2");
        printJson(hbaseDao.put(map));
    }

    @Test
    //@Ignore
    public void batchPut() {
        int count = 200;
        ExtendsHbaseMap<Object>[] batch = new ExtendsHbaseMap[count];
        for (int start = 3, i = start; i < count + start; i++) {
            ExtendsHbaseMap<Object> map = new ExtendsHbaseMap<>();
            map.put("age", 1 + ThreadLocalRandom.current().nextInt(60));
            map.put("name", "name"+i);
            map.put("rowKey", "name"+i);
            batch[i - start] = map;
        }
        printJson(hbaseDao.put(batch));
    }

    @Test
    public void get() {
        printJson(hbaseDao.get("ponfee"));
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
        printJson(hbaseDao.range("name89", "name95"));
    }

    @Test
    public void find() {
        printJson(hbaseDao.find("name89", "name94", 20));
        printJson(hbaseDao.find("name89", "name94", 2));
        printJson(hbaseDao.find("name94", "name89", 20, true));
        printJson(hbaseDao.find("name94", "name89", 2, true));
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
        printJson(hbaseDao.nextPage(query));
    }

    @Test
    public void nextPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "name" }));
        List<ExtendsHbaseMap<Object>> data = new ArrayList<>();
        int count = 1;
        List<ExtendsHbaseMap<Object>> list = (List<ExtendsHbaseMap<Object>>) hbaseDao.nextPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.nextPageStartRow(list).get(ROW_KEY_NAME));
            query.setStartRow((String) query.nextPageStartRow(list).get(ROW_KEY_NAME));
            list = (List<ExtendsHbaseMap<Object>>) hbaseDao.nextPage(query);
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
    public void nextPageAllDESC() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "name" }));
        List<ExtendsHbaseMap<Object>> data = new ArrayList<>();
        int count = 1;
        List<ExtendsHbaseMap<Object>> list = (List<ExtendsHbaseMap<Object>>) hbaseDao.nextPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.nextPageStartRow(list).get(ROW_KEY_NAME));
            query.setStartRow((String) query.nextPageStartRow(list).get(ROW_KEY_NAME));
            list = (List<ExtendsHbaseMap<Object>>) hbaseDao.nextPage(query);
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
        List<ExtendsHbaseMap<Object>> data = new ArrayList<>();
        int count = 1;
        List<ExtendsHbaseMap<Object>> list = (List<ExtendsHbaseMap<Object>>) hbaseDao.previousPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.previousPageStartRow(list).get(ROW_KEY_NAME));
            query.setStartRow((String) query.previousPageStartRow(list).get(ROW_KEY_NAME));
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
    public void previousPageAllDesc() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "name" }));
        query.setStartRow("name10");
        List<ExtendsHbaseMap<Object>> data = new ArrayList<>();
        int count = 1;
        List<ExtendsHbaseMap<Object>> list = (List<ExtendsHbaseMap<Object>>) hbaseDao.previousPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.previousPageStartRow(list).get(ROW_KEY_NAME));
            query.setStartRow((String) query.previousPageStartRow(list).get(ROW_KEY_NAME));
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
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "age" }));
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
        printJson(hbaseDao.prefix("name10", PAGE_SIZE));
    }

    @Test
    public void regexp() {
        //printJson(extendsHbaseDao1.regexp("^name.*1$", "name10", PAGE_SIZE));
        printJson(hbaseDao.regexp("^name.*1$", 2));
    }

    @Test
    public void delete() {
        printJson(hbaseDao.get("ponfee1"));
        printJson(hbaseDao.delete(new String[] { "ponfee1","ponfee2" }));
        printJson(hbaseDao.get("ponfee2"));
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
