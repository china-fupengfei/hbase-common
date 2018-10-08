package code.ponfee.hbase.other;

import org.springframework.stereotype.Repository;

import code.ponfee.hbase.HbaseDao;

@Repository("extendsHbaseMapDao")
public class ExtendsHbaseMapDao extends HbaseDao<ExtendsHbaseMap<Object>> {

}
