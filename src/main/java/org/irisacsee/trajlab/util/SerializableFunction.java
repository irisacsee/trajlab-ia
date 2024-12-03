package org.irisacsee.trajlab.util;

import java.io.Serializable;

@FunctionalInterface
public interface SerializableFunction<T, R> extends Serializable {
    R apply(T t);
}
