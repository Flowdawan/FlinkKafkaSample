package io.redpanda.examples;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;
import org.mongoflink.serde.DocumentSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Map;


public class StringDocumentSerializer implements DocumentSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(StringDocumentSerializer.class);
    @Override
    public Document serialize(Object o) {

        // Json String to Object
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = null;

        String name = "";

        try {
            rootNode = mapper.readTree(o.toString());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        if (rootNode != null) {
            String lastname = rootNode.findValue("name").findValue("first").toString();
            String firstname = rootNode.findValue("name").findValue("last").toString();
            name = lastname.concat(firstname);
        }

        // creating the hash
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        messageDigest.update(name.getBytes());
        String stringHash = new String(messageDigest.digest());

        // creating the document for the mongodb
        final Document doc = new Document("name", stringHash);
        final String jsonString = doc.toJson();
        final Document doc2 = Document.parse(jsonString);
        LOG.info(String.valueOf(doc2));

        return doc2;

    }
}
