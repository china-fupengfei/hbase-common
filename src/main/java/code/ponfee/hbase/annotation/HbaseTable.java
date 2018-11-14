package code.ponfee.hbase.annotation;

import java.lang.annotation.*;

/**
 * Mapped by hbase table name
 * 
 * @author Ponfee
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

    /** the table-level hbase family name */
    String family() default "";

    /** whether serial row key */
    boolean serialRowKey() default false;
}
