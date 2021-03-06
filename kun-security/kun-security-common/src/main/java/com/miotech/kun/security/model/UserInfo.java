package com.miotech.kun.security.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

@Data
public class UserInfo implements Serializable {

    @JsonSerialize(using= ToStringSerializer.class)
    private Long id;

    @JsonProperty(required = true)
    private String username;

    @JsonProperty(required = true, access = JsonProperty.Access.WRITE_ONLY)
    private String password;

    private Set<String> permissions;

    private AuthenticationOriginInfo authOriginInfo;

    private String firstName;

    private String lastName;

    private String email;

    private Long userGroupId;

}
