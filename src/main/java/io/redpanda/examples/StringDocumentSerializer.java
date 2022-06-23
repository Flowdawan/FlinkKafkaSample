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
import java.nio.charset.StandardCharsets;
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
        String age = "";
        String country = "";
        String postcode = "";

        try {
            rootNode = mapper.readTree(o.toString());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        if (rootNode != null) {
            String lastname = rootNode.findValue("name").findValue("first").toString();
            String firstname = rootNode.findValue("name").findValue("last").toString();
            name = lastname.concat(firstname);

            age = rootNode.findValue("dob").findValue("age").toString();
            country = rootNode.findValue("country").toString().replace("\"", "");
            postcode = rootNode.findValue("postcode").toString();

        }

        // creating the hash
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        byte[] hashName = digest.digest(name.getBytes(StandardCharsets.UTF_8));

        /*
        byte[] hashAge = digest.digest(age.getBytes(StandardCharsets.UTF_8));
        byte[] hashCountry = digest.digest(country.getBytes(StandardCharsets.UTF_8));
        byte[] hashPostcode = digest.digest(postcode.getBytes(StandardCharsets.UTF_8));
        */

        // creating the document for the mongodb
        final Document doc = new Document("name", hashName).append("age", age).append("country", country).append("postcode", postcode);
        final String jsonString = doc.toJson();
        final Document doc2 = Document.parse(jsonString);
        LOG.info(String.valueOf(doc2));

        return doc2;

    }
}
