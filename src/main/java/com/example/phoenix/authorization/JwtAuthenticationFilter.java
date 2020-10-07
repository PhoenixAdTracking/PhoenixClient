package com.example.phoenix.authorization;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import lombok.NonNull;
import org.apache.http.HttpHeaders;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import javax.crypto.SecretKey;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Date;

public class JwtAuthenticationFilter extends UsernamePasswordAuthenticationFilter {

    private String jwtAudience;
    private String jwtIssuer;
    private String jwtSecret;
    private String jwtType;

    public JwtAuthenticationFilter(
            @NonNull final AuthenticationManager authenticationManager,
            @NonNull final String jwtAudience,
            @NonNull final String jwtIssuer,
            @NonNull final String jwtSecret,
            @NonNull final String jwtType) {
        this.jwtAudience = jwtAudience;
        this.jwtIssuer = jwtIssuer;
        this.jwtSecret = jwtSecret;
        this.jwtType = jwtType;
        this.setAuthenticationManager(authenticationManager);
        setFilterProcessesUrl("/login");
    }

    @Override
    public Authentication attemptAuthentication(HttpServletRequest request, HttpServletResponse response) throws AuthenticationException {
        return super.attemptAuthentication(request, response);
    }

    @Override
    protected boolean requiresAuthentication(HttpServletRequest request, HttpServletResponse response) {
        return super.requiresAuthentication(request, response);
    }

    @Override
    protected void unsuccessfulAuthentication(HttpServletRequest request, HttpServletResponse response, AuthenticationException failed) throws IOException, ServletException {
        super.unsuccessfulAuthentication(request, response, failed);
        response.addHeader("hello", "hello");
    }

    @Override
    protected void successfulAuthentication(
            @NonNull final HttpServletRequest request,
            @NonNull final HttpServletResponse response,
            @NonNull final FilterChain chain,
            @NonNull final Authentication authResult) throws IOException, ServletException {
        final User user = (User) authResult.getPrincipal();
        final SecretKey secretKey = Keys.hmacShaKeyFor(jwtSecret.getBytes());
        final String token = Jwts.builder()
                .signWith(secretKey, SignatureAlgorithm.HS512)
                .setHeaderParam("type", jwtType)
                .setIssuer(jwtIssuer)
                .setAudience(jwtAudience)
                .setSubject(user.getUsername())
                .setExpiration(new Date(System.currentTimeMillis() + 864000000))
                .compact();
        response.addHeader(HttpHeaders.AUTHORIZATION, token);
    }
}