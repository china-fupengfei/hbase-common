package com.sf.sids.hbase.other;

import org.springframework.stereotype.Repository;

import com.sf.sids.hbase.HbaseDao;

@Repository("extendsHbaseMapDao")
public class ExtendsHbaseMapDao extends HbaseDao<ExtendsHbaseMap<Object>> {

}
