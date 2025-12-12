package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB Driver for Custom Python Coordinator (Lab 3).
 */
public class LabDBClient extends DB {

    private String coordinatorUrl;
    private static final String TABLE_NAME = "usertable"; // Стандартна назва в YCSB

    @Override
    public void init() throws DBException {
        Properties props = getProperties();
        // Отримуємо URL з командного рядка або дефолт
        coordinatorUrl = props.getProperty("labdb.url", "http://localhost:8000");
        
        System.out.println("LabDBClient initialized. Target: " + coordinatorUrl);
        
        // Спробуємо створити таблицю (ігноруємо помилки, якщо вже є)
        try {
            sendRequest("POST", "/tables", "{\"name\": \"" + TABLE_NAME + "\"}");
        } catch (Exception e) {
            // Table might exist
        }
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
            // GET /tables/{table}/records/{key}
            String endpoint = "/tables/" + table + "/records/" + key;
            String jsonResponse = sendRequest("GET", endpoint, null);

            if (jsonResponse == null) return Status.NOT_FOUND;

            // Відповідь API: {"value": {"field1": "data", ...}, "version": 1}
            // Нам треба дістати те, що всередині "value"
            
            // Дуже примітивний парсинг JSON, щоб не підключати Jackson/Gson
            // Шукаємо "value": { ... }
            int valIndex = jsonResponse.indexOf("\"value\"");
            if (valIndex == -1) return Status.ERROR;

            // Просто повертаємо весь JSON як одне поле, бо YCSB це дозволяє
            result.put("data", new StringByteIterator(jsonResponse));
            return Status.OK;

        } catch (Exception e) {
            if (e.getMessage().contains("404")) return Status.NOT_FOUND;
            e.printStackTrace();
            return Status.ERROR;
        }
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        try {
            // Формуємо JSON: {"partition_key": "key", "value": { ... }}
            StringBuilder valueJson = new StringBuilder("{");
            for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
                valueJson.append("\"").append(entry.getKey()).append("\": \"")
                         .append(entry.getValue().toString()).append("\",");
            }
            if (values.size() > 0) valueJson.setLength(valueJson.length() - 1); // remove last comma
            valueJson.append("}");

            String payload = String.format(
                "{\"partition_key\": \"%s\", \"value\": %s}", 
                key, valueJson.toString()
            );

            sendRequest("POST", "/tables/" + table + "/records", payload);
            return Status.OK;

        } catch (Exception e) {
            e.printStackTrace();
            return Status.ERROR;
        }
    }

    @Override
    public Status delete(String table, String key) {
        try {
            sendRequest("DELETE", "/tables/" + table + "/records/" + key, null);
            return Status.OK;
        } catch (Exception e) {
            return Status.ERROR;
        }
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        return insert(table, key, values); // Upsert logic
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return Status.NOT_IMPLEMENTED;
    }

    // --- Helper for pure Java HTTP (No heavy deps) ---
    private String sendRequest(String method, String path, String body) throws Exception {
        URL url = new URL(coordinatorUrl + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        
        if (body != null) {
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes());
            }
        }

        int code = conn.getResponseCode();
        if (code >= 400) {
            if (code == 404) throw new RuntimeException("404");
            throw new RuntimeException("HTTP Error " + code);
        }

        try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) response.append(line);
            return response.toString();
        }
    }
}
