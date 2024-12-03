package org.irisacsee.trajlab.util;

import java.io.Serializable;

@FunctionalInterface
public interface SerializableBiFunction<T, U, R> extends Serializable {
    R apply(T t, U u);
}
