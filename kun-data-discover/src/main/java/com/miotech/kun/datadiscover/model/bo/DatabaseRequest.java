package com.miotech.kun.datadiscover.model.bo;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.json.simple.JSONObject;

import java.util.List;

/**
 * @author: Melo
 * @created: 5/26/20
 */

@Data
@NoArgsConstructor
public class DatabaseRequest {

    private Long typeId;

    private String name;

    private JSONObject information;

    private List<String> tags;

    private String createUser;

    private Long createTime;

    private String updateUser;

    private Long updateTime;
}
