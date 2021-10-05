package com.drobot.kstream.serde;

import com.drobot.kstream.entity.booking.ExtendedBooking;
import com.drobot.kstream.entity.booking.LightBooking;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class JsonSerdeFactory {

    public static Serde<ExtendedBooking> extendedBookingSerde() {
        return fromClass(ExtendedBooking.class);
    }

    public static Serde<LightBooking> lightBookingSerde() {
        return fromClass(LightBooking.class);
    }

    private static <T> Serde<T> fromClass(Class<T> klass) {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(klass));
    }
}
