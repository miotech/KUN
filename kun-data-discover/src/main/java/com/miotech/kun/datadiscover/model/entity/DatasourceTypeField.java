package com.miotech.kun.datadiscover.model.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.Data;

/**
 * @author: Jie Chen
 * @created: 6/12/20
 */
@Data
public class DatasourceTypeField {

    @JsonSerialize(using= ToStringSerializer.class)
    private Long id;

    @JsonSerialize(using= ToStringSerializer.class)
    private Long typeId;

    @JsonProperty("key")
    private String name;

    @JsonProperty("order")
    private Integer sequenceOrder;

    private String format;

    private Boolean require;
}
