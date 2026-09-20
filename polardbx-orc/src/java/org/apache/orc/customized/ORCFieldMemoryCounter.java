package org.apache.orc.customized;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ORCFieldMemoryCounter {
    boolean value() default true;
}