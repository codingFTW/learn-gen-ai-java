package com.athingforcode.llm4j.app;

import com.athingforcode.llm4j.service.*;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class SearchUsingJinaColbertV2 {

    public static void main(String[] args) throws Exception {
        final var client = new QdrantClient(QdrantGrpcClient.newBuilder("localhost", 6334, false).build());
        final String collectionName = "jinacolbertv2faqcollection";

        //This demonstrate querying documents using Jina Colbert V2 Embedding as documented here :
        //https://jina.ai/news/jina-colbert-v2-multilingual-late-interaction-retriever-for-embedding-and-reranking/
        searchJinaColbertV2AppStart(client,collectionName);

        /* Execute this line if you would like to create new empty Qdrant multivector collection
        QdrantOperation.createQDrantCollectionMultiVectorEmbeddingUsingClient(client,"jinacolbertv2faqcollection");
        */
        /* Execute this line if you would like to insert FAQ to Qdrant multivector collection
        QdrantOperation.insertFAQToQdrantMultiVectorEmbeddingCollection(client,"jinacolbertv2faqcollection","faq_prod.jsonl");
        */
    }

    private static void searchJinaColbertV2AppStart(QdrantClient client, String collectionName) throws IOException, ExecutionException, InterruptedException {
        Scanner inputPrompt = new Scanner(System.in);
        System.out.println("Type your search query (or :q to quit)");

        while (true) {
            System.out.print("Query : ");
            String searchQuery = inputPrompt.nextLine().trim();
            if (searchQuery.equals(":q")) {
                System.out.println("Exiting the program. Goodbye!");
                break;
            }
            var startTime = System.currentTimeMillis();
            //search indexed FAQ stored in Qdrant
            var searchResult = JinaColbertV2Search.semanticJinaColbertV2FAQSearch(searchQuery,client,5,collectionName);
            Util.printElapsedTime(startTime, "search faq");
            searchResult.ifPresentOrElse(list -> list.forEach(System.out::println),
                    () -> System.out.println("Search query did not match any faq"));
        }

        inputPrompt.close();
    }

}
