package com.drobot.beam.function;

import java.io.Serializable;

public interface SerDe<Representation, JavaObject> extends Serializable {

    JavaObject deserialize(Representation representation);
    Representation serialize(JavaObject javaObject);
}
