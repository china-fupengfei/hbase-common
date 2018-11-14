package code.ponfee.hbase.annotation;

import java.lang.annotation.*;

/**
 * Entity field mapping to hbase column
 * 
 * @author Ponfee
 */
@Documented
@Inherited
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface HbaseField {

    /** ignore as hbase qualifier */
    boolean ignore() default false;

    /** whether serial field value */
    boolean serial() default false;

    /** the column-level hbase family name. */
    String family() default "";

    /** the hbase qualifier name. */
    String qualifier() default "";

    /** the hbase value format, to compatible multiple date format then is array. */
    String[] format() default {};

    // ---------------------------------------public static final field
    String FORMAT_TIMESTAMP = "timestamp";
}
