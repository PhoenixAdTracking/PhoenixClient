package com.example.phoenix;

import com.example.phoenix.ingestion.PhoenixDataProcessor;
import com.example.phoenix.models.Business;
import com.example.phoenix.models.User;
import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.sql.SQLException;

@RestController
public class PhoenixClientController {

    @Resource(name = "PhoenixDB")
    private BasicDataSource phoenixDb;

    private PhoenixDataProcessor dataProcessor;

    @PostConstruct
    public void setup() {
        this.dataProcessor = new PhoenixDataProcessor(phoenixDb);
    }

    @GetMapping("/ping")
    public String ping() {
        return "pinged!";
    }

    @PostMapping("/user")
    public int postUser(@RequestBody User user) throws SQLException {
        return dataProcessor.createUser(user);
    }

    @PostMapping("/business")
    public int postBusiness(@RequestBody Business business) throws SQLException {
        return dataProcessor.createBusiness(business);
    }
}
