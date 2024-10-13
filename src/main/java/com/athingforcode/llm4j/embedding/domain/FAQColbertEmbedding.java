package com.athingforcode.llm4j.embedding.domain;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

public record FAQColbertEmbedding(String id, String title, String description, ZonedDateTime startDate, ZonedDateTime endDate, List<List<Float>> embedding) {
    public FAQColbertEmbedding {
        Objects.requireNonNull(id);
        Objects.requireNonNull(title);
        Objects.requireNonNull(description);
        Objects.requireNonNull(startDate);
        Objects.requireNonNull(endDate);
        Objects.requireNonNull(embedding);
    }
}
