package com.miotech.kun.datadiscover.model.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: Jie Chen
 * @created: 6/12/20
 */
@Data
public class DatasourceType {

    @JsonSerialize(using= ToStringSerializer.class)
    private Long id;

    @JsonProperty("type")
    private String name;

    List<DatasourceTypeField> fields = new ArrayList<>();

    public void addField(DatasourceTypeField field) {
        fields.add(field);
    }
}
