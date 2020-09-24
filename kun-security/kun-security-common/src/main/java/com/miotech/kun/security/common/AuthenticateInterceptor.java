package com.miotech.kun.security.common;

import com.miotech.kun.common.model.RequestResult;
import com.miotech.kun.security.SecurityContextHolder;
import com.miotech.kun.security.model.bo.UserInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.StringJoiner;

/**
 * @author: Jie Chen
 * @created: 2020/9/22
 */
@Slf4j
public class AuthenticateInterceptor extends HandlerInterceptorAdapter {

    private String securityBaseUrl;

    /**
     * separate from app rest template
     */
    RestTemplate restTemplate = new RestTemplate();

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        try {
            return doAuthenticate(request);
        } catch (Exception e) {
            log.error("Failed to authenticate.", e);
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return false;
        }
    }

    private boolean doAuthenticate(HttpServletRequest request) {
        StringJoiner cookieStrBuilder = new StringJoiner(";");
        for (Cookie cookie : request.getCookies()) {
            String cookieFullStr = cookie.getName() + "=" + cookie.getValue();
            cookieStrBuilder.add(cookieFullStr);
        }
        String passToken = request.getHeader(ConfigKey.REQUEST_PASS_TOKEN_KEY);
        if (StringUtils.isEmpty(passToken)) {
            passToken = request.getParameter(ConfigKey.REQUEST_PASS_TOKEN_KEY);
        }
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.COOKIE, cookieStrBuilder.toString());
        headers.add(ConfigKey.REQUEST_PASS_TOKEN_KEY, passToken);
        HttpEntity entity = new HttpEntity(headers);
        String authUrl = securityBaseUrl + "/security/whoami";
        ResponseEntity<RequestResult<UserInfo>> authResult = restTemplate.exchange(authUrl,
                HttpMethod.GET,
                entity,
                new ParameterizedTypeReference<RequestResult<UserInfo>>() {});
        if (!authResult.getStatusCode().is2xxSuccessful()) {
            return false;
        }
        UserInfo userInfo = authResult.getBody().getResult();
        SecurityContextHolder.setUserInfo(userInfo);
        return true;
    }

    public void setSecurityBaseUrl(String securityBaseUrl) {
        this.securityBaseUrl = securityBaseUrl;
    }
}