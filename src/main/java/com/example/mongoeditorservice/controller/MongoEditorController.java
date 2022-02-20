package com.example.mongoeditorservice.controller;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api")
@Slf4j
public class MongoEditorController {

    @Autowired
    MongoClient mongoClient;

    @GetMapping("/getdbs")
    public Map<String, Set<String>> getAllDatabases() {

        Map<String, Set<String>> dbs = new TreeMap<>();
        ArrayList<Document> dbDocs = mongoClient.listDatabases().into(new ArrayList<>());
        for (Document dbDoc : dbDocs) {
            Set<String> collections = new TreeSet<>();
            String dbName = dbDoc.getString("name");
            MongoDatabase db = mongoClient.getDatabase(dbName);
            ArrayList<Document> collectionDocs = db.listCollections().into(new ArrayList<>());
            for (Document collection : collectionDocs) {
                collections.add(collection.getString("name"));
            }

            dbs.put(dbName, collections);
        }
        return dbs;
    }

    @GetMapping("/collection/{dbName}/{collectionName}")
    public List<Document> getCollection(@PathVariable("dbName") String dbName,
                                        @PathVariable("collectionName") String collectionName) {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        ArrayList<Document> documents = collection.find().into(new ArrayList<>());
        return documents;
    }

    @PostMapping("/collection/search/{dbName}/{collectionName}")
    public List<Document> searchCollection(@PathVariable("dbName") String dbName,
                                           @PathVariable("collectionName") String collectionName,
                                           @RequestBody String searchPayload) {
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        BsonDocument searchDoc = BsonDocument.parse(searchPayload);
        log.info("searchDoc :: {}", searchDoc.toJson());

        ArrayList<Document> documents = collection.find(searchDoc).into(new ArrayList<>());
        return documents;
    }

    @PostMapping("/collection/update/{dbName}/{collectionName}/{idFieldName}/{id}")
    public Document updateCollection(
            @PathVariable("dbName") String dbName,
            @PathVariable("collectionName") String collectionName,
            @PathVariable("idFieldName") String idFieldName,
            @PathVariable("id") String id,
            @RequestBody Document document
    ) {

        log.info("document :: {}", document.toJson());
        log.info("id1 :: {}", id);
        log.info("id2 :: {}", document.get("_id"));

        if (idFieldName.equals("id")) {
            idFieldName = "_id";
        }

        if (document == null || document.get(idFieldName) == null || document.get(idFieldName).toString().isEmpty()
                || !id.equals(document.get(idFieldName).toString()))
            new IllegalArgumentException("Id hasn't matched");

        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection collection = database.getCollection(collectionName);

        return (Document) collection.findOneAndUpdate(new Document(idFieldName, id), document);
    }

    @PostMapping("/collection/insert/{dbName}/{collectionName}/{idFieldName}/{id}")
    public InsertOneResult insertCollection(
            @PathVariable("dbName") String dbName,
            @PathVariable("collectionName") String collectionName,
            @PathVariable("idFieldName") String idFieldName,
            @PathVariable("id") String id,
            @RequestBody Document document
    ) {
        log.info("document :: {}", document.toJson());
        log.info("id2 :: {}", document.get("_id"));

        if (idFieldName.equals("id")) {
            idFieldName = "_id";
        }

        if (document == null || document.get(idFieldName) == null || document.get(idFieldName).toString().isEmpty()
                || !document.get(idFieldName).toString().equals(id))
            throw new IllegalArgumentException("Id hasn't matched");

        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection collection = database.getCollection(collectionName);

        Document idDocument = null;
        if (document.get(idFieldName) instanceof Integer)
            idDocument = new Document(idFieldName, Integer.parseInt(document.get(idFieldName).toString()));
        else
            idDocument = new Document(idFieldName, document.get(idFieldName).toString());

        log.info("idDocument :: {}", idDocument.toJson());
        Document filteredDocuments = (Document) collection.find(idDocument).first();

        log.info("filteredDocuments :: {}", (filteredDocuments == null ? null : filteredDocuments.toJson()));

        if (filteredDocuments != null) {
            log.info("Found doc...");
            throw new IllegalArgumentException("Id already exist");
        }

        log.info("Inserting...");
        return collection.insertOne(document);
    }
}
