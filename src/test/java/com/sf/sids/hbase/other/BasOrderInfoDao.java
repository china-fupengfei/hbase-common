package com.sf.sids.hbase.other;

import org.springframework.stereotype.Repository;

import com.sf.sids.hbase.HbaseDao;

@Repository("basOrderInfoDao")
public class BasOrderInfoDao extends HbaseDao<BasOrderInfo> {

}
