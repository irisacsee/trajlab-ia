package org.irisacsee.trajlab.index;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * 索引类型枚举
 *
 * @author irisacsee
 * @since 2024/11/20
 */
public enum IndexType implements Serializable {
    // spatial only
    XZ2(0),
    XZT(1),
    ID(2),
    // Concatenate temporal index before spatial index
    TXZ2(3),
    // Concatenate spatial index before temporal index
    XZ2T(4),
    // Index value will be car ids
    OBJECT_ID_T(5),
    KNN(6),
    BUFFER(7),
    SIMILAR(8),
    ACCOMPANY(9);

    final int id;

    public static List<IndexType> spatialIndexTypes() {
        return Arrays.asList(XZ2, XZ2T, TXZ2);
    }

    IndexType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}