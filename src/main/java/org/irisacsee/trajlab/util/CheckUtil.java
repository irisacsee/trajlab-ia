package org.irisacsee.trajlab.util;

import org.apache.commons.lang.NullArgumentException;

import java.util.Collection;

/**
 * 检查工具
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public class CheckUtil {
    public static void checkEmpty(Object... o) {
        for (Object object : o) {
            if (object == null) {
                throw new NullArgumentException("The parameter can not be null!");
            }
        }
    }

    public static boolean isCollectionEmpty(Collection var0) {
        return var0 == null || var0.isEmpty();
    }
}
