package com.github.lisama569.localmessagequeuejava.mq.handler;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON 序列化门面（持久化与 HTTP 复用）。
 */
public final class Json {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Json() {
    }

    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 序列化失败: " + e.getMessage(), e);
        }
    }

    public static <T> T fromJson(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 反序列化失败: " + e.getMessage(), e);
        }
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }
}
