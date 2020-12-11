package com.google.cloud.dataflow.cdc.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.Optional;

public class ObjectHelper {

    private ObjectHelper() {
    }

    public static Optional<String> convertToString(final Serializable object) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            return Optional.of(Base64.getEncoder().encodeToString(baos.toByteArray()));
        } catch (final IOException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }

    public static byte[] convertToByteArray(final Serializable object) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            return baos.toByteArray();
        } catch (final IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static <T extends Serializable> Object convertFromByteArray(final byte[] object) {
        try (final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(object))) {
            return ois.readObject();
        } catch (final IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static <T extends Serializable> Optional<T> convertFrom(final String objectAsString) {
        final byte[] data = Base64.getDecoder().decode(objectAsString);
        try (final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return Optional.of((T) ois.readObject());
        } catch (final IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }
}
