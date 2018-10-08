package com.sf.sids.hbase.test;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.alibaba.fastjson.JSONObject;
import com.sf.sids.hbase.other.ExtendsHbaseMapDao;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath*:test-hbase.xml" })
public class HbaeDaoDDLTest {

    private @Resource ExtendsHbaseMapDao hbaseDao;

    @Test
    public void createTable() {
        System.out.println(hbaseDao.createTable());
    }

    public static void printJson(Object obj) {
        System.out.println(JSONObject.toJSONString(obj));
    }

}
