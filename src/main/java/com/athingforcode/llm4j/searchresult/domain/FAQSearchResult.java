package com.athingforcode.llm4j.searchresult.domain;

import java.util.Objects;

public record FAQSearchResult(String id, String title, String description, double searchScore) {
    public FAQSearchResult {
        Objects.requireNonNull(id);
        Objects.requireNonNull(title);
        Objects.requireNonNull(description);
    }
}
