package com.athingforcode.llm4j.service;

import com.athingforcode.llm4j.embedding.domain.FAQColbertEmbedding;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.VectorFactory;
import io.qdrant.client.grpc.Collections;
import io.qdrant.client.grpc.JsonWithInt;
import io.qdrant.client.grpc.Points;
import org.json.JSONObject;

import java.io.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static io.qdrant.client.PointIdFactory.id;

public class QdrantOperation {
    private static final String jakartaZoneId = "GMT+7";

    public static void createQDrantCollectionMultiVectorEmbeddingUsingClient(QdrantClient client, String collectionName) throws ExecutionException, InterruptedException {
        client.createCollectionAsync(collectionName,
                Collections.VectorParams.newBuilder()
                        .setDistance(Collections.Distance.Dot)
                        .setSize(128)
                        .setMultivectorConfig(Collections.MultiVectorConfig.newBuilder()
                                .setComparator(Collections.MultiVectorComparator.MaxSim)
                                .build())
                        .build()).get();
    }

    public static void insertFAQToQdrantMultiVectorEmbeddingCollection(QdrantClient client, String collectionName, String originFilePath) throws ExecutionException, InterruptedException, IOException {
        var faqs = createJinaColbertV2FAQJsonlFile(originFilePath).orElse(java.util.Collections.emptyList());
        var idCounter = new AtomicLong(1);
        var points = faqs.stream()
                .map(faq -> Points.PointStruct.newBuilder()
                        .setId(id(idCounter.getAndIncrement()))
                        .setVectors(Points.Vectors.newBuilder().setVector(VectorFactory.multiVector(faq.embedding())))
                        .putPayload("faq_id", JsonWithInt.Value.newBuilder().setStringValue(faq.id()).build())
                        .putPayload("title", JsonWithInt.Value.newBuilder().setStringValue(faq.title()).build())
                        .putPayload("description", JsonWithInt.Value.newBuilder().setStringValue(faq.description()).build())
                        .putPayload("start_date", JsonWithInt.Value.newBuilder().setStringValue(stringFromDateTime(faq.startDate())).build())
                        .putPayload("end_date", JsonWithInt.Value.newBuilder().setStringValue(stringFromDateTime(faq.endDate())).build())
                        .build()).toList();

        client
            .upsertAsync(collectionName, points)
            .get();
    }

    private static Optional<List<FAQColbertEmbedding>> createJinaColbertV2FAQJsonlFile(String originFilePath) throws IOException {
        List<FAQColbertEmbedding> faqs = null;
        try (BufferedReader br = new BufferedReader(new FileReader(originFilePath))) {
            String line;
            int lineCounter = 1;

            while (((line = br.readLine()) != null) && !line.trim().isEmpty()) {
                if (lineCounter >=1) {
                    try {
                        System.out.println("Processing line-" + lineCounter);
                        JSONObject jsonObject = new JSONObject(line);

                        String faqId = jsonObject.getString("id");
                        String title = jsonObject.getString("title");
                        String desc = jsonObject.getString("desc");
                        var faqEmbedding = JinaColbertV2Search.createJinaColbertV2Embedding(title + " " + desc, false);
                        ZonedDateTime startDate = unixTimeStamptoZonedDateTime(jsonObject.getLong("start_date"),jakartaZoneId);
                        ZonedDateTime endDate = unixTimeStamptoZonedDateTime(jsonObject.getLong("end_date"),jakartaZoneId);

                        if (faqs == null) faqs = new ArrayList<>();
                        faqs.add(
                                new FAQColbertEmbedding(faqId, title, desc, startDate,endDate, faqEmbedding)
                        );
                    }
                    catch(Exception ex) {
                        System.out.println("terjadi exception di line-" + lineCounter);
                    }
                }
                lineCounter++;
            }
        }
        return Optional.ofNullable(faqs);
    }

    private static String stringFromDateTime(ZonedDateTime dateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
        return dateTime.format(formatter);
    }

    private static ZonedDateTime unixTimeStamptoZonedDateTime(long unixTimestamp, String timezone) {
        return Instant.ofEpochSecond(unixTimestamp).atZone(ZoneId.of(timezone));
    }
}
