package com.epam.lambda;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.util.Base64;
import org.apache.http.HttpStatus;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class Handler implements RequestStreamHandler {
    private static final String GUID = "guid";
    private static final String DATA = "data";
    private static final String FILE_NAME_PATTERN = "%s.xml";
    private static final String BUCKET_NAME = System.getenv("bucketName");
    private static final String DYNAMO_DB_TABLE_NAME = System.getenv("tableName");

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        JSONObject response = new JSONObject();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            JSONParser parser = new JSONParser();
            JSONObject event = (JSONObject) parser.parse(reader);
            Map<String, String> tags;

            if (event.get(DATA) != null) {
                String xmlFileData = (String) event.get(DATA);

                XmlParser xmlParser = new XmlParser();
                tags = xmlParser.parseXml(xmlFileData);

                if (tags != null && tags.get(GUID) != null) {
                    putFileIntoS3Bucket(xmlFileData, tags.get(GUID));
                    saveTagsInDynamoDB(tags);
                    response.put("statusCode", HttpStatus.SC_OK);
                } else {
                    response.put("statusCode", HttpStatus.SC_BAD_REQUEST);
                }
            } else {
                response.put("statusCode", HttpStatus.SC_BAD_REQUEST);
            }
        } catch (Exception e) {
            response.put("statusCode", HttpStatus.SC_BAD_REQUEST);
            response.put("exception", e.getMessage());
        }

        OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
        writer.write(response.toString());
        writer.close();
    }

    private void putFileIntoS3Bucket(String xmlFileData, String guid) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        ObjectMetadata fileMetadata = new ObjectMetadata();
        fileMetadata.setContentType("application/xml");
        String fileName = String.format(FILE_NAME_PATTERN, guid);
        s3Client.putObject(BUCKET_NAME, fileName, new ByteArrayInputStream(Base64.decode(xmlFileData)), fileMetadata);
    }

    private void saveTagsInDynamoDB(Map<String, String> tags) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        Map<String, AttributeValue> attributes = new HashMap<>();
        tags.forEach((key, value) -> attributes.put(key, new AttributeValue().withS(value)));
        PutItemRequest putItemRequest = new PutItemRequest().withTableName(DYNAMO_DB_TABLE_NAME).withItem(attributes);
        client.putItem(putItemRequest);
    }
}