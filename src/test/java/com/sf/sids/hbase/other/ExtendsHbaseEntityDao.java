package com.sf.sids.hbase.other;

import org.springframework.stereotype.Repository;

import com.sf.sids.hbase.HbaseDao;

@Repository("extendsHbaseEntityDao")
public class ExtendsHbaseEntityDao extends HbaseDao<ExtendsHbaseEntity> {

}
