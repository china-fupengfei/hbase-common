package code.ponfee.hbase.other;

import org.springframework.stereotype.Repository;

import code.ponfee.hbase.HbaseDao;

@Repository("basOrderInfoDao")
public class BasOrderInfoDao extends HbaseDao<BasOrderInfo> {

}
