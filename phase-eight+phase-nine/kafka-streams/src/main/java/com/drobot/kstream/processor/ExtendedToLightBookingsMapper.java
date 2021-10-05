package com.drobot.kstream.processor;

import com.bazaarvoice.jolt.JsonUtils;
import com.bazaarvoice.jolt.Removr;
import com.drobot.kstream.entity.booking.ExtendedBooking;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.lang.reflect.Field;
import java.util.*;

public class ExtendedToLightBookingsMapper implements ValueMapper<String, String> {

    @Override
    public String apply(String value) {
        Map<String, Object> input = convertJsonToMap(value);
        Map<String, Object> spec = createRemovrSpec();
        Map<String, Object> transformedInput = removeFields(input, spec);
        return convertMapToJson(transformedInput);
    }

    private Map<String, Object> convertJsonToMap(String json) {
        return JsonUtils.jsonToMap(json);
    }

    private String convertMapToJson(Map<String, Object> map) {
        return JsonUtils.toJsonString(map);
    }

    private Map<String, Object> createRemovrSpec() {
        Map<String, Object> spec = new HashMap<>();
        Field[] fields = ExtendedBooking.JsonField.class.getFields();
        Field[] superFields = ExtendedBooking.JsonField.class.getSuperclass().getFields();
        List<Field> fieldList = new ArrayList<>(Arrays.asList(fields));
        fieldList.removeAll(Arrays.asList(superFields));
        try {
            for (Field field : fieldList) {
                spec.put((String) field.get(null), "");
            }
        } catch (IllegalAccessException ignored) {
        }
        return spec;
    }

    private Map<String, Object> removeFields(Map<String, Object> input, Map<String, Object> spec) {
        Removr removr = new Removr(spec);
        removr.transform(input);
        return input;
    }
}
