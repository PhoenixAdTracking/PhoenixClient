package com.example.phoenix;

import com.example.phoenix.ingestion.ExternalDataFetcher;
import com.example.phoenix.ingestion.PhoenixDataProcessor;
import com.example.phoenix.models.Business;
import com.example.phoenix.models.InsightType;
import com.example.phoenix.models.Insights;
import com.example.phoenix.models.User;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.sql.SQLException;
import java.util.List;

@RestController
public class PhoenixClientController {

    @Resource(name = "PhoenixDB")
    private PhoenixDataProcessor dataProcessor;

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

    @GetMapping("/campaign")
    public List<Insights> getCampaigns(
            @RequestParam String accessToken,
            @RequestParam String adAccountId) throws Exception {
        return dataProcessor.getInsights(adAccountId, accessToken, InsightType.CAMPAIGN);
    }

    @GetMapping("/adset")
    public List<Insights> getAdsets(
            @RequestParam String accessToken,
            @RequestParam String adAccountId) throws Exception {
        return dataProcessor.getInsights(adAccountId, accessToken, InsightType.AD_SET);
    }

    @GetMapping("/ad")
    public List<Insights> getAds(
            @RequestParam String accessToken,
            @RequestParam String adAccountId) throws Exception {
        return dataProcessor.getInsights(adAccountId, accessToken, InsightType.AD);
    }
}
