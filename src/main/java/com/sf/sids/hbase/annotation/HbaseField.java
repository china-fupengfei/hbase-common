package com.sf.sids.hbase.annotation;

import java.lang.annotation.*;

/**
 * Entity field mapping to hbase column
 * 
 * @author 01367825
 */
@Documented
@Inherited
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface HbaseField {

    /** ignore as hbase qualifier */
    boolean ignore() default false;

    /** serial object */
    boolean serial() default false;

    /** the hbase family name. */
    String family() default "";

    /** the hbase qualifier name. */
    String qualifier() default "";

    /** the hbase value format. */
    String[] format() default {};

    // ---------------------------------------public static final field
    String FORMAT_TIMESTAMP = "timestamp";
}
