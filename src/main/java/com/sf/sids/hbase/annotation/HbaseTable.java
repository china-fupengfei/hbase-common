package com.sf.sids.hbase.annotation;

import java.lang.annotation.*;

/**
 * Mapping the hbase table name
 * 
 * @author 01367825
 */
@Documented
@Target(ElementType.TYPE)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface HbaseTable {

    /** hbase table namespace */
    String namespace() default "";

    /** hbase table name, default LOWER_UNDERSCORE(Class.getSimpleName()) */
    String tableName() default "";

    /** the class level family name for hbase */
    String family() default "";
}
