package org.irisacsee.trajlab.conf;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

/**
 * 配置类
 *
 * @author irisacsee
 * @since 2024/07/22
 */
@Data
public class Config {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    @JsonProperty
    private ILoadConfig loadConfig;
    @JsonProperty
    private IDataConfig dataConfig;
    @JsonProperty
    private IStoreConfig storeConfig;

    public static Config parse(String content) throws JsonProcessingException {
        return MAPPER.readValue(content, Config.class);
    }
}
