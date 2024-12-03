package org.irisacsee.trajlab.conf;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/11/14
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME
)
@JsonSubTypes({
        @JsonSubTypes.Type(
                value = StandaloneLoadConfig.class,
                name = "standalone"
        )
})

public interface ILoadConfig extends Serializable {
    InputType getInputType();

    String getFsDefaultName();

    enum InputType implements Serializable {
        STANDALONE("standalone"),
        HDFS("hdfs"),
        HBASE("hbase"),
        HIVE("hive"),
        GEOMESA("geomesa");

        private String inputType;

        InputType(String inputType) {
            this.inputType = inputType;
        }

        public final String toString() {
            return "InputType{type='" + this.inputType + '\'' + '}';
        }
    }
}
