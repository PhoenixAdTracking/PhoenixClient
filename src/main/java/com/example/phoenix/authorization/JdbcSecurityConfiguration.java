package com.example.phoenix.authorization;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

@EnableWebSecurity
public class JdbcSecurityConfiguration extends WebSecurityConfigurerAdapter {

    private String jwtSecret;
    @Value("${jwt.issuer}")
    private String jwtIssuer;
    @Value("${jwt.type}")
    private String jwtType;
    @Value("${jwt.audience}")
    private String jwtAudience;

    private JdbcTemplate template;

    @Autowired
    private BasicDataSource dataSource;

    @Bean (name = "PasswordEncoder")
    public PasswordEncoder passwordEncoder() {
        final BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();
        return new BCryptPasswordEncoder();
    }

    @Bean
    public JdbcTemplate databaseTemplate() {
        return new JdbcTemplate(dataSource);
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        jwtSecret = System.getenv("JWT_SECRET");

        http.cors().and()
                .addFilter(new JwtAuthenticationFilter(
                        authenticationManager(),
                        jwtAudience,
                        jwtIssuer,
                        jwtSecret,
                        jwtType))
                .addFilter(new JwtAuthorizationFilter(
                        authenticationManager(),
                        databaseTemplate(),
                        jwtAudience,
                        jwtIssuer,
                        jwtSecret,
                        jwtType))
                .authorizeRequests(authorizeRequests ->
                        authorizeRequests
                                .antMatchers("/register/**", "/login", "/ping", "/event/**").permitAll()
                                .antMatchers("/insights/**").hasAuthority("USER"))
                .httpBasic().realmName("phoenix")
                .and()
                .sessionManagement()
                .sessionCreationPolicy(SessionCreationPolicy.STATELESS);

    }

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth
                .jdbcAuthentication()
                .dataSource(dataSource)
                .passwordEncoder(passwordEncoder())
                .usersByUsernameQuery("SELECT username, password, enabled from users where username = ?")
                .authoritiesByUsernameQuery("SELECT username, authority from users where username = ?");
    }
}
