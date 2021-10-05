package com.drobot.kstream;

import java.io.*;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

@SuppressWarnings("unused")
public class ApplicationProperties extends Properties {

    public static class Key {
        public static final String BOOKINGS_TOPIC = "topic.bookings";
        public static final String LIGHT_BOOKINGS_TOPIC = "topic.lightBookings";
        public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
        public static final String APPLICATION_ID = "application.id";
    }

    private static final String PROPERTIES_FILE = "application.properties";

    private static ApplicationProperties instance;

    private final Properties properties;

    private ApplicationProperties() throws IOException {
        properties = loadProperties();
    }

    public static ApplicationProperties getInstance() {
        if (instance == null) {
            try {
                instance = new ApplicationProperties();
            } catch (IOException e) {
                throw new RuntimeException("Unable to load " + PROPERTIES_FILE + " file", e);
            }
        }
        return instance;
    }

    public String getBookingsTopic() {
        return getProperty(Key.BOOKINGS_TOPIC);
    }

    public String getLightBookingsTopic() {
        return getProperty(Key.LIGHT_BOOKINGS_TOPIC);
    }

    public String getBootstrapServers() {
        return getProperty(Key.BOOTSTRAP_SERVERS);
    }

    public String getApplicationId() {
        return getProperty(Key.APPLICATION_ID);
    }

    @Override
    public Object setProperty(String key, String value) {
        return properties.setProperty(key, value);
    }

    @Override
    public void load(Reader reader) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void load(InputStream inStream) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("deprecation")
    @Override
    @Deprecated
    public void save(OutputStream out, String comments) {
        properties.save(out, comments);
    }

    @Override
    public void store(Writer writer, String comments) throws IOException {
        properties.store(writer, comments);
    }

    @Override
    public void store(OutputStream out, String comments) throws IOException {
        properties.store(out, comments);
    }

    @Override
    public void loadFromXML(InputStream in) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void storeToXML(OutputStream os, String comment) throws IOException {
        properties.storeToXML(os, comment);
    }

    @Override
    public void storeToXML(OutputStream os, String comment, String encoding) throws IOException {
        properties.storeToXML(os, comment, encoding);
    }

    @Override
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    @Override
    public Enumeration<?> propertyNames() {
        return properties.propertyNames();
    }

    @Override
    public Set<String> stringPropertyNames() {
        return properties.stringPropertyNames();
    }

    @Override
    public void list(PrintStream out) {
        properties.list(out);
    }

    @Override
    public void list(PrintWriter out) {
        properties.list(out);
    }

    @Override
    public int size() {
        return properties.size();
    }

    @Override
    public boolean isEmpty() {
        return properties.isEmpty();
    }

    @Override
    public Enumeration<Object> keys() {
        return properties.keys();
    }

    @Override
    public Enumeration<Object> elements() {
        return properties.elements();
    }

    @Override
    public boolean contains(Object value) {
        return properties.contains(value);
    }

    @Override
    public boolean containsValue(Object value) {
        return properties.containsValue(value);
    }

    @Override
    public boolean containsKey(Object key) {
        return properties.containsKey(key);
    }

    @Override
    public Object get(Object key) {
        return properties.get(key);
    }

    @Override
    public Object put(Object key, Object value) {
        return properties.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return properties.remove(key);
    }

    @Override
    public void putAll(Map<?, ?> t) {
        properties.putAll(t);
    }

    @Override
    public void clear() {
        properties.clear();
    }

    @Override
    public Object clone() {
        return properties.clone();
    }

    @Override
    public String toString() {
        return properties.toString();
    }

    @Override
    public Set<Object> keySet() {
        return properties.keySet();
    }

    @Override
    public Set<Map.Entry<Object, Object>> entrySet() {
        return properties.entrySet();
    }

    @Override
    public Collection<Object> values() {
        return properties.values();
    }

    @Override
    public Object getOrDefault(Object key, Object defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super Object, ? super Object> action) {
        properties.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super Object, ? super Object, ?> function) {
        properties.replaceAll(function);
    }

    @Override
    public Object putIfAbsent(Object key, Object value) {
        return properties.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return properties.remove(key, value);
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
        return properties.replace(key, oldValue, newValue);
    }

    @Override
    public Object replace(Object key, Object value) {
        return properties.replace(key, value);
    }

    @Override
    public Object computeIfAbsent(Object key, Function<? super Object, ?> mappingFunction) {
        return properties.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public Object computeIfPresent(Object key, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        return properties.computeIfPresent(key, remappingFunction);
    }

    @Override
    public Object compute(Object key, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        return properties.compute(key, remappingFunction);
    }

    @Override
    public Object merge(Object key, Object value, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        return properties.merge(key, value, remappingFunction);
    }

    @Override
    public int hashCode() {
        return properties.hashCode();
    }

    private static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(ApplicationProperties.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE));
        return properties;
    }
}
