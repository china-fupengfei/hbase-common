package code.ponfee.hbase.test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;

import code.ponfee.hbase.model.PageQueryBuilder;
import code.ponfee.hbase.other.BasOrderInfoDao;
import code.ponfee.hbase.other.CopyOrderInfo;
import code.ponfee.hbase.other.CopyOrderInfoDao;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:test-hbase.xml" })
public class CopyOrderInfoTest {

    private static final int PAGE_SIZE = 5000;
    private @Resource BasOrderInfoDao basHbaseDao;
    private @Resource CopyOrderInfoDao copyHbaseDao;

    @Test
    public void tableExists() {
        System.out.println(copyHbaseDao.tableExists());
    }

    @Test
    public void dropTable() {
        System.out.println(copyHbaseDao.dropTable());
    }

    @Test
    public void createTable() {
        System.out.println(copyHbaseDao.createTable());
    }

    @Test
    public void descTable() {
        System.out.println(copyHbaseDao.descTable());
    }

    @Test
    public void get() {
        printJson(copyHbaseDao.get("abc"));
    }

    @Test
    public void first() {
        printJson(copyHbaseDao.first());
    }

    @Test
    public void last() {
        printJson(copyHbaseDao.last());
    }

    @Test
    public void range() {
        printJson(copyHbaseDao.range(null, null));
    }

    @Test
    public void find() {
        printJson(copyHbaseDao.find("abc", 20));
    }

    @Test
    public void nextPageAll() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.setFamQuaes(ImmutableMap.of("cf1", new String[] { "name" }));
        List<CopyOrderInfo> data = new ArrayList<>();
        int count = 1;
        List<CopyOrderInfo> list = (List<CopyOrderInfo>) copyHbaseDao.nextPage(query);
        while (CollectionUtils.isNotEmpty(list) && list.size() == query.getPageSize()) {
            count++;
            data.addAll(list);
            printJson(list);
            printJson((String) query.nextPageStartRow(list).getRowKey());
            query.setStartRowKey((String) query.nextPageStartRow(list).getRowKey());
            list = (List<CopyOrderInfo>) copyHbaseDao.nextPage(query);
        }
        if (CollectionUtils.isNotEmpty(list)) {
            data.addAll(list);
        }
        Set<String> set = new LinkedHashSet<>();
        //Set<String> set = new TreeSet<>();
        data.stream().forEach(m -> set.add((String) m.getRowKey()));
        System.out.println("======================count: " + count);
        System.out.println("======================" + set.size());
        printJson(set);
    }

    @Test
    public void count() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        printJson("======================" + copyHbaseDao.count(query));
    }

    @Test
    public void copy() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.setRowKeyPrefix("a");
        query.setStartRowKey("a20160401_S1603310002862_03.21.3213102-T", true);
        query.setStopRowKey("a20160401_S1603310004352_03.21.3211104-W");
        basHbaseDao.copy(query, copyHbaseDao, t -> {
            t.setModelId(1);
            t.buildRowKey();
        });
    }

    @Test
    public void copy2() {
        PageQueryBuilder query = PageQueryBuilder.newBuilder(PAGE_SIZE);
        query.setRowKeyPrefix("a");
        String startRow = basHbaseDao.nextRowKey("a", "a20160401_"); // 可以直接写a20160401_
        String stopRow = basHbaseDao.previousRowKey("a", "a20160403_");
        query.setStartRowKey(startRow, true);
        query.setStopRowKey(stopRow);
        basHbaseDao.copy(query, copyHbaseDao, t -> {
            t.setModelId(1);
            t.buildRowKey();
        });
    }

    @Test
    public void nextRowKey() {
        // abc
        printJson(basHbaseDao.nextRowKey("a", "a20160401_"));
    }

    @Test
    public void previousRowKey() {
        // a20160402_S1604050001672_03.24.3241104-P
        printJson(basHbaseDao.previousRowKey("a", "a20160403_"));

        // a20180926_S1809260015952_07.07.7705017
        printJson(basHbaseDao.previousRowKey("a", "a20360403_"));
    }

    @Test
    public void previousRowKey2() {
        // a20160402_S1604050001672_03.24.3241104-P
        printJson(basHbaseDao.previousRowKey("a", "a20160402_", 50));

        // a20180926_S1809260015952_07.07.7705017
        printJson(basHbaseDao.previousRowKey("a", "a20460400_", 50));

        // a20000402_
        printJson(basHbaseDao.previousRowKey("a", "a20000402_", 50));
    }

    @Test
    public void previousRowKey3() {
        // 6_57095810-6_20180926_CK2018092691_JXJ605060038
        printJson(basHbaseDao.previousRowKey(null, null));

        printJson(basHbaseDao.previousRowKey("a", null)); // 全表扫描
    }

    // -------------------------------------------------------------------------------
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
