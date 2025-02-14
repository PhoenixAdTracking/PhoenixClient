package com.example.phoenix.ingestion;

import com.example.phoenix.models.Insights;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.common.collect.ImmutableList;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@NoArgsConstructor
public class GoogleSheetsService {
    private static final String APPLICATION_NAME = "Phoenix";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final String TOKENS_DIRECTORY_PATH = "tokens";
    private static final String SHEET_ID = "15mwOp3INAgKvwnD0VeYCdLPPU3vT0uyc2Fr0c-Z1xn8";
    private static final List<Object> HEADER_FIELDS = ImmutableList.of(
            "Type",
            "Id",
            "Name",
            "Spend",
            "Impressions",
            "Frequency",
            "Clicks",
            "CPM",
            "CPC",
            "CTR",
            "Platform Purchases",
            "Tracked Purchases",
            "Platform CPA",
            "Tracked CPA",
            "Platform CVR",
            "Tracked CVR",
            "Total Sales (In Dollars)",
            "ROAS",
            "ROI");

    /**
     * Global instance of the scopes required by this quickstart.
     * If modifying these scopes, delete your previously saved tokens/ folder.
     */
    private static final List<String> SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS);
    private static final String CREDENTIALS_FILE_PATH = "/credentials.json";

    private Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
        // Load client secrets.
        InputStream in = GoogleSheetsService.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
        if (in == null) {
            throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
        }
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    }

    private Sheets sheets() throws IOException, GeneralSecurityException {
        // Build a new authorized API client service.
        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        return new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    public void setSheetInfo(@NonNull final List<Insights> insights) throws IOException, GeneralSecurityException {
        final List<List<Object>> insightBody = insights.stream()
                .map(insight -> new ImmutableList.Builder<Object>()
                        .add(insight.getType().toString())
                        .add(insight.getId())
                        .add(insight.getName())
                        .add(String.valueOf(insight.getSpend()))
                        .add(String.valueOf(insight.getImpressions()))
                        .add(String.valueOf(insight.getFrequency()))
                        .add(String.valueOf(insight.getClicks()))
                        .add(String.valueOf(insight.getCpm()))
                        .add(String.valueOf(insight.getCpc()))
                        .add(String.valueOf(insight.getCtr()))
                        .add(String.valueOf(insight.getFbPurchases()))
                        .add(String.valueOf(insight.getPhoenixPurchases()))
                        .add(String.valueOf(insight.getFbCpa()))
                        .add(String.valueOf(insight.getCpa()))
                        .add(String.valueOf(insight.getFbCvr()))
                        .add(String.valueOf(insight.getCvr()))
                        .add(String.valueOf(insight.getTotalSales()))
                        .add(String.valueOf(insight.getRoas()))
                        .add(String.valueOf(insight.getRoi()))
                        .build())
                .collect(Collectors.toList());
        final List<List<Object>> bodyValues =
                new ImmutableList.Builder<List<Object>>()
                        .add(HEADER_FIELDS)
                        .addAll(insightBody)
                        .build();
        final ValueRange body = new ValueRange().setValues(bodyValues);
        sheets().spreadsheets().values().update(SHEET_ID, "A1", body)
                .setValueInputOption("RAW")
                .execute();
    }
}
