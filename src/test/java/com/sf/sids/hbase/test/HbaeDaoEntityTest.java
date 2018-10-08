package com.sf.sids.hbase.test;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import com.sf.sids.hbase.bean.PageQueryBuilder;
import com.sf.sids.hbase.bean.PageSortOrder;
import com.sf.sids.hbase.other.ExtendsHbaseEntity;
import com.sf.sids.hbase.other.ExtendsHbaseEntityDao;

import code.ponfee.commons.util.Dates;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:test-hbase.xml" })
public class HbaeDaoEntityTest {
    private static final int PAGE_SIZE = 50;
    private @Resource ExtendsHbaseEntityDao hbaseDao;

    @Test
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
    //@Ignore
    public void batchPut() {
        int count = 200;
        ExtendsHbaseEntity[] batch = new ExtendsHbaseEntity[count];
        for (int start = 3, i = start; i < count + start; i++) {
            ExtendsHbaseEntity entity = new ExtendsHbaseEntity();
            //entity.setFirstName(RandomStringUtils.randomAlphabetic(3));
            //entity.setLastName(RandomStringUtils.randomAlphabetic(3));
            entity.setFirstName("fu");
            entity.setLastName("ponfee");
            entity.setAge(ThreadLocalRandom.current().nextInt(60));
            entity.setBirthday(Dates.random(Dates.toDate("20000101", "yyyyMMdd")));
            entity.buildRowKey();
            batch[i - start] = entity;
        }
        printJson(hbaseDao.put(batch));
    }

    @Test
    public void get() {
        printJson(hbaseDao.get("fu_ponfee_20000102"));
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
        printJson(hbaseDao.range("fu_ponfee_20000101", "fu_ponfee_20090101"));
    }

    @Test
    public void find() {
        printJson(hbaseDao.find("fu_ponfee_20000101", "fu_ponfee_20090101", 2000));
        printJson(hbaseDao.find("fu_ponfee_20000101", "fu_ponfee_20090101", 2));
        printJson(hbaseDao.find("fu_ponfee_20090101", "fu_ponfee_20000101", 2000, true));
        printJson(hbaseDao.find("fu_ponfee_20090101", "fu_ponfee_20000101", 2, true));
    }

    @Test
    public void findAll() {
        List<ExtendsHbaseEntity> list = (List<ExtendsHbaseEntity>) hbaseDao.range(null, null);
        System.out.println("======================" + list.size());
        printJson(list);
    }

    @Test
    public void nextPage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.ASC);
        printJson(hbaseDao.nextPage(query));
    }

    @Test
    public void nextPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.ASC);
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "first_name" }));
        List<ExtendsHbaseEntity> data = new ArrayList<>();
        int count = 1;
        List<ExtendsHbaseEntity> list = (List<ExtendsHbaseEntity>) hbaseDao.nextPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.nextPageStartRow(list).getRowKey());
            query.setStartRow((String) query.nextPageStartRow(list).getRowKey());
            list = (List<ExtendsHbaseEntity>) hbaseDao.nextPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.getRowKey()));
        System.out.println("======================round: " + count);
        System.out.println("======================" + set.size());
        printJson(set);
    }

    @Test
    public void nextPageAllDesc() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.setRowKeyPrefix("fu_ponfee_201");
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "first_name" }));
        List<ExtendsHbaseEntity> data = new ArrayList<>();
        int count = 1;
        List<ExtendsHbaseEntity> list = (List<ExtendsHbaseEntity>) hbaseDao.nextPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.nextPageStartRow(list).getRowKey());
            query.setStartRow((String) query.nextPageStartRow(list).getRowKey());
            list = (List<ExtendsHbaseEntity>) hbaseDao.nextPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.getRowKey()));
        System.out.println("======================round: " + count);
        System.out.println("======================" + set.size());
        printJson(set);
    }
    
    @Test
    public void previousPage() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.setStartRow("fu_ponfee_20000331");
        printJson(hbaseDao.previousPage(query));
    }
    
    @Test
    public void previousPageDesc() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        printJson(hbaseDao.previousPage(query));
    }

    @Test
    public void previousPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.setStartRow("fu_ponfee_20181128");
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "first_name" }));
        List<ExtendsHbaseEntity> data = new ArrayList<>();
        int count = 1;
        List<ExtendsHbaseEntity> list = (List<ExtendsHbaseEntity>) hbaseDao.previousPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.previousPageStartRow(list).getRowKey());
            query.setStartRow((String) query.previousPageStartRow(list).getRowKey());
            list = (List<ExtendsHbaseEntity>) hbaseDao.previousPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.getRowKey()));
        System.out.println("======================round: " + count);
        System.out.println("======================" + set.size());
        printJson(set);
    }

    @Test
    public void previousPageAllDesc() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE, PageSortOrder.DESC);
        query.setStartRow("fu_ponfee_20010101");
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "first_name" }));
        List<ExtendsHbaseEntity> data = new ArrayList<>();
        int count = 1;
        List<ExtendsHbaseEntity> list = (List<ExtendsHbaseEntity>) hbaseDao.previousPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count ++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.previousPageStartRow(list).getRowKey());
            query.setStartRow((String) query.previousPageStartRow(list).getRowKey());
            list = (List<ExtendsHbaseEntity>) hbaseDao.previousPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String)m.getRowKey()));
        System.out.println("======================round: " + count);
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
        //query.setRowKeyPrefix("fu_ponfee_201");
        query.setMaxResultSize(0);
        printJson("======================" + hbaseDao.count(query));
    }

    // -------------------------------------------------------------------------------
    @Test
    public void prefix() {
        //printJson(extendsHbaseDao1.prefix("name10", "name10", PAGE_SIZE));
        printJson(hbaseDao.prefix("fu_ponfee_201", PAGE_SIZE));
    }

    @Test
    public void regexp() {
        //printJson(extendsHbaseDao1.regexp("^name.*1$", "name10", PAGE_SIZE));
        printJson(hbaseDao.regexp("^fu_ponfee_2\\d{2}1.*1$", 2));
    }

    @Test
    public void delete() {
        printJson(hbaseDao.get("fu_ponfee_20011031"));
        printJson(hbaseDao.delete(new String[] { "fu_ponfee_20011031","fu_ponfee_20110531" }));
        printJson(hbaseDao.get("fu_ponfee_20110531"));
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
    
    public static void main(String[] args) {
        System.out.println(FastDateFormat.getInstance("yyyyMMdd").format(new Date()));
        System.out.println(FastDateFormat.getInstance("yyyyMMdd").format(new Date()));
    }

}
