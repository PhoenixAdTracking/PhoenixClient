package com.example.phoenix;

import com.example.phoenix.ingestion.PhoenixDataProcessor;
import com.example.phoenix.models.*;
import lombok.NonNull;
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

    @PostMapping("/register/user")
    public int postUser(@RequestBody User user) throws SQLException {
        return dataProcessor.createUser(user);
    }

    @PostMapping("/register/business")
    public int postBusiness(@RequestBody Business business) throws SQLException {
        return dataProcessor.createBusiness(business);
    }

    @GetMapping("/insights/multi/campaigns")
    public List<Insights> getCampaignsFromAdAccount(
            @RequestParam String accessToken,
            @RequestParam String adAccountId) throws Exception {
        return dataProcessor.getInsights(adAccountId, accessToken, InsightType.CAMPAIGN);
    }

    @GetMapping("/insights/multi/adsets")
    public List<Insights> getAdsetsFromCampaign(
            @RequestParam String accessToken,
            @RequestParam String adsetId) throws Exception {
        return dataProcessor.getInsights(adsetId, accessToken, InsightType.AD_SET);
    }

    @GetMapping("/insights/multi/ads")
    public List<Insights> getAdsFromAdset(
            @RequestParam String accessToken,
            @RequestParam String adId) throws Exception {
        return dataProcessor.getInsights(adId, accessToken, InsightType.AD);
    }

    @PostMapping("/event/click")
    public long postEvent(@NonNull @RequestBody EventPost eventPost) throws MissingEventInfoException, SQLException {
        return dataProcessor.processClickEvent(eventPost);
    }
}
