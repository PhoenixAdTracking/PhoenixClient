package com.example.phoenix;

import com.example.phoenix.models.Business;
import com.example.phoenix.models.User;
import org.springframework.web.bind.annotation.*;

@RestController
public class PhoenixClientController {

    @GetMapping("/ping")
    public String ping() {
        return "pinged!";
    }

    @PostMapping("/business")
    public int postUser(@RequestBody User user) {
        return user.getId();
    }

    @PostMapping("/user")
    public int postBusiness(@RequestBody Business business) {
        return business.getId();
    }
}
