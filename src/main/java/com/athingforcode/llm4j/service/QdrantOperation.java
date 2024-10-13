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


    /**
     * Creates a QDrant collection with multi-vector embedding configuration using the provided QdrantClient.
     *
     * @param client The QdrantClient instance to use for creating the collection.
     * @param collectionName The name of the collection to be created.
     * @throws ExecutionException If the execution of the asynchronous operation fails.
     * @throws InterruptedException If the current thread is interrupted while waiting for the asynchronous operation to complete.
     */
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

    /**
     * Inserts FAQ data into a QDrant collection with multi-vector embedding.
     *
     * @param client The QdrantClient instance to use for inserting data.
     * @param collectionName The name of the collection to insert data into.
     * @param originFilePath The file path of the origin data in JSONL format.
     * @throws ExecutionException If the execution of the asynchronous operation fails.
     * @throws InterruptedException If the current thread is interrupted while waiting for the asynchronous operation to complete.
     * @throws IOException If an I/O error occurs while reading the origin file.
     */
    public static void insertFAQToQdrantMultiVectorEmbeddingCollection(QdrantClient client, String collectionName, String originFilePath) throws ExecutionException, InterruptedException, IOException {
        var faqs = readFAQJSONlFile(originFilePath).orElse(java.util.Collections.emptyList());
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

    /**
     * Creates a list of FAQColbertEmbedding objects from a JSONL file.
     *
     * @param originFilePath The file path of the origin data in JSONL format.
     * @return An Optional containing a List of FAQColbertEmbedding objects if successful, or an empty Optional if the file is empty or an error occurs.
     * @throws IOException If an I/O error occurs while reading the origin file.
     */
    private static Optional<List<FAQColbertEmbedding>> readFAQJSONlFile(String originFilePath) throws IOException {
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

    /**
     * Converts a ZonedDateTime object to a formatted string.
     *
     * @param dateTime The ZonedDateTime object to be converted.
     * @return A string representation of the date and time in the format "yyyy-MM-dd HH:mm:ss z".
     */
    private static String stringFromDateTime(ZonedDateTime dateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
        return dateTime.format(formatter);
    }

    /**
     * Converts a Unix timestamp to a ZonedDateTime object.
     *
     * @param unixTimestamp The Unix timestamp to be converted.
     * @param timezone The timezone ID string for the resulting ZonedDateTime.
     * @return A ZonedDateTime object representing the given Unix timestamp in the specified timezone.
     */
    private static ZonedDateTime unixTimeStamptoZonedDateTime(long unixTimestamp, String timezone) {
        return Instant.ofEpochSecond(unixTimestamp).atZone(ZoneId.of(timezone));
    }
}
