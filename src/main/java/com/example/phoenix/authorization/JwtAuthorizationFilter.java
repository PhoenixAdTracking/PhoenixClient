package com.example.phoenix.authorization;

import com.google.common.collect.ImmutableList;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.springframework.http.HttpHeaders;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class JwtAuthorizationFilter extends BasicAuthenticationFilter {

    private String jwtSecret;
    private String jwtIssuer;
    private String jwtType;
    private String jwtAudience;
    private JdbcTemplate jdbcTemplate;

    public JwtAuthorizationFilter(AuthenticationManager authenticationManager, JdbcTemplate jdbcTemplate,
                                  String jwtAudience, String jwtIssuer, String jwtSecret, String jwtType) {
        super(authenticationManager);
        this.jdbcTemplate = jdbcTemplate;
        this.jwtAudience = jwtAudience;
        this.jwtIssuer = jwtIssuer;
        this.jwtSecret = jwtSecret;
        this.jwtType = jwtType;
    }

    @Override
    protected void onUnsuccessfulAuthentication(HttpServletRequest request, HttpServletResponse response, AuthenticationException failed) throws IOException {
        super.onUnsuccessfulAuthentication(request, response, failed);
    }



    private UsernamePasswordAuthenticationToken parseToken(HttpServletRequest request) {
        String token = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (token != null && token.startsWith("Bearer ")) {
            String claims = token.replace("Bearer ", "");
            try {
                Jws<Claims> claimsJws = Jwts.parser()
                        .setSigningKey(jwtSecret.getBytes())
                        .parseClaimsJws(claims);

                String username = claimsJws.getBody().getSubject();

                if ("".equals(username) || username == null) {
                    return null;
                }

                final List<SimpleGrantedAuthority> authorities = jdbcTemplate
                        .queryForList("SELECT authority from users where username = ?", String.class, username)
                        .stream()
                        .map(SimpleGrantedAuthority::new)
                        .collect(Collectors.toList());

                return new UsernamePasswordAuthenticationToken(username, null, authorities);
            } catch (JwtException exception) {
            }
        }

        return null;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws IOException, ServletException {
        UsernamePasswordAuthenticationToken authentication = parseToken(request);

        if (authentication != null) {
            SecurityContextHolder.getContext().setAuthentication(authentication);
        } else {
            SecurityContextHolder.clearContext();
        }

        filterChain.doFilter(request, response);
    }
}
