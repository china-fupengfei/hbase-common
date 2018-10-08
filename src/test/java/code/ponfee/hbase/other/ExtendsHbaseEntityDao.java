package code.ponfee.hbase.other;

import org.springframework.stereotype.Repository;

import code.ponfee.hbase.HbaseDao;

@Repository("extendsHbaseEntityDao")
public class ExtendsHbaseEntityDao extends HbaseDao<ExtendsHbaseEntity> {

}
