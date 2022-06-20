package io.redpanda.examples;

import com.esotericsoftware.minlog.Log;
import com.google.gson.Gson;
import org.bson.Document;
import org.mongoflink.serde.DocumentSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class StringDocumentSerializer implements DocumentSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(StringDocumentSerializer.class);
    @Override
    public Document serialize(Object o) {

        // Converting input into java object
        Gson gson = new Gson();
        KafkaStream ks = gson.fromJson(toString(), KafkaStream.class);
        LOG.info(ks.getName());

        // creating the hash
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        String myTestString = "test";
        messageDigest.update(myTestString.getBytes());
        String stringHash = new String(messageDigest.digest());
        LOG.info(stringHash);

        // creating the document for the mongodb
        final Document doc = new Document("name", myTestString);
        final String jsonString = doc.toJson();
        final Document doc2 = Document.parse(jsonString);
        return doc2;

    }
}
