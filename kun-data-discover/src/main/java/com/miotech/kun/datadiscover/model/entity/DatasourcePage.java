package com.miotech.kun.datadiscover.model.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.miotech.kun.datadiscover.model.PageInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author: Jie Chen
 * @created: 6/12/20
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class DatasourcePage extends PageInfo {

    @JsonProperty("databases")
    private List<Datasource> datasources;
}
