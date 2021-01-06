package com.example.phoenix;

import com.example.phoenix.ingestion.PhoenixDataProcessor;
import com.example.phoenix.models.*;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.sql.SQLException;
import java.util.List;

@CrossOrigin("*")
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
        return dataProcessor.createBusiness(business.getName());
    }

    @GetMapping("/insights/multi/campaigns")
    public List<Insights> getCampaignsFromAdAccount(@RequestBody CampaignsRequest request) throws Exception {
        return dataProcessor.getInsights(request.getAdAccountId(), request.getAccessToken(), InsightType.CAMPAIGN);
    }

    @GetMapping("/insights/multi/adsets")
    public List<Insights> getAdsetsFromCampaign(@RequestBody AdsetsRequest request) throws Exception {
        return dataProcessor.getInsights(request.getCampaignId(), request.getAccessToken(), InsightType.AD_SET);
    }

    @GetMapping("/insights/multi/ads")
    public List<Insights> getAdsFromAdset(@RequestBody AdsRequest request) throws Exception {
        return dataProcessor.getInsights(request.getAdsetId(), request.getAccessToken(), InsightType.AD);
    }

    @PostMapping("/event/visit")
    public long postVisitEvent(@RequestBody EventPost eventPost) throws MissingEventInfoException, SQLException {
        return dataProcessor.processClickEvent(eventPost);
    }

    @PostMapping("/event/purchase")
    public long postPurchaseEvent(@RequestBody EventPost eventPost) throws MissingEventInfoException, SQLException {
        return dataProcessor.processPurchaseEvent(eventPost);
    }
}
