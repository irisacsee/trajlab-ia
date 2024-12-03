package org.irisacsee.trajlab.conf;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * 存储配置接口
 *
 * @author irisacsee
 * @since 2024/11/22
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({@JsonSubTypes.Type(value = HBaseStoreConfig.class, name = "hbase")})
public interface IStoreConfig extends Serializable {}
