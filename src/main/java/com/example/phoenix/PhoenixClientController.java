package com.example.phoenix;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PhoenixClientController {

    @GetMapping("/ping")
    public String ping() {
        return "pinged!";
    }
}
