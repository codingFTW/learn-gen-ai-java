package com.athingforcode.llm4j.service;

import com.athingforcode.llm4j.searchresult.domain.FAQSearchResult;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.grpc.JsonWithInt;
import io.qdrant.client.grpc.Points;
import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static io.qdrant.client.QueryFactory.nearestMultiVector;
import static io.qdrant.client.WithPayloadSelectorFactory.enable;

public class JinaColbertV2Search {

    // TODO you can get your free Jina AI API Key from : https://jina.ai/embeddings/
    private static final String jinaAIKey = "<insert your jina ai_key here>";
    public static Optional<List<FAQSearchResult>> semanticJinaColbertV2FAQSearch(String searchQuery, QdrantClient client, int topK, String  collectionName) throws IOException, ExecutionException, InterruptedException {

        var queryEmbedding = createJinaColbertV2Embedding(searchQuery, true);
        List<Points.ScoredPoint> result = client.queryAsync(Points.QueryPoints.newBuilder()
                .setCollectionName(collectionName)
                .setLimit(topK)
                .setQuery(nearestMultiVector(queryEmbedding))
                .setWithPayload(enable(true))
                .build()).get();

        List<FAQSearchResult> searchResult = result.stream()
                .map(point -> new FAQSearchResult(
                        point.getPayloadOrDefault("faq_id", JsonWithInt.Value.newBuilder().setStringValue("").build()).getStringValue(),
                        point.getPayloadOrDefault("title", JsonWithInt.Value.newBuilder().setStringValue("").build()).getStringValue(),
                        point.getPayloadOrDefault("description", JsonWithInt.Value.newBuilder().setStringValue("").build()).getStringValue(),
                        point.getScore()
                )).toList();

        return Optional.of(searchResult);
    }

    public static List<List<Float>> createJinaColbertV2Embedding(String text, boolean isQuery) throws IOException {
        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(60, TimeUnit.SECONDS)
                .writeTimeout(60, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS)
                .build();

        final Request request = createJinaColbertV2EmbeddingRequest(text, isQuery);

        String responseBody;
        try (Response response = client.newCall(request).execute()) {

            if (!response.isSuccessful()) {
                throw new IOException("Unexpected response code: " + response);
            }

            responseBody = Objects.requireNonNull(response.body()).string();
        }
        JSONObject jsonResponse = new JSONObject(responseBody);
        JSONArray embeddingArray = jsonResponse.getJSONArray("data")
                .getJSONObject(0)
                .getJSONArray("embeddings");

        return IntStream
                .range(0, embeddingArray.length())
                .mapToObj(outerindex -> IntStream
                        .range(0, embeddingArray.getJSONArray(outerindex).length())
                        .mapToObj(innerindex -> embeddingArray.getJSONArray(outerindex).getFloat(innerindex))
                        .toList()
                )
                .toList();
    }

    private static Request createJinaColbertV2EmbeddingRequest(String text, boolean isQuery) {
        final String JINA_API_EMBEDDING_URL = "https://api.jina.ai/v1/multi-vector";

        JSONArray inputs = new JSONArray(Collections.singletonList(text));
        JSONObject jsonBody = new JSONObject()
                .put("input", inputs)
                .put("model", "jina-colbert-v2")
                .put("dimensions", 128)
                .put("embedding_type", "float");
        if (isQuery) {
            jsonBody.put("input_type", "query");
        } else {
            jsonBody.put("input_type", "document");
        }

        RequestBody body = RequestBody.create(
                jsonBody.toString(),
                MediaType.parse("application/json; charset=utf-8")
        );

        return new Request.Builder()
                .url(JINA_API_EMBEDDING_URL)
                .addHeader("Authorization", "Bearer %s".formatted(jinaAIKey))
                .post(body)
                .build();
    }
}
