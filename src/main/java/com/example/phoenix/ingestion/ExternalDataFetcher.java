package com.example.phoenix.ingestion;

import com.facebook.ads.sdk.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NoArgsConstructor
public class ExternalDataFetcher {

    /**
     * Method for pulling a user's Ad Accounts and returning them as a Map of the account's name to its Id.
     * @param accessToken The access token for pulling this user's info.
     * @param userId The id of the User whose ad accounts need to be pulled.
     * @return A map of ad accounts' names to ids.
     * @throws Exception
     */
    public Map<String, String> getFacebookAdAccounts(
            @NonNull final String accessToken,
            @NonNull final String userId) throws Exception{
        final APIContext context = new APIContext(accessToken);
        final User user = new User(userId, context);
        final List<AdAccount> adAccounts = user.getAdAccounts().requestNameField().requestIdField().execute();
        return adAccounts.stream()
                .collect(Collectors.toMap(
                        adAccount ->
                                adAccount.getFieldName(),
                        adAccount ->
                                adAccount.getFieldId()));

    }
}
