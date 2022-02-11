package com.bayer.datahub;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.InvocationTargetException;

public class ReflectionHelper {

    /**
     * Read value of a private field (including superclass fields).
     */
    public static <T> T readObjectField(Object object, String fieldName) {
        try {
            //noinspection unchecked
            return (T) FieldUtils.readField(object, fieldName, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T readStaticField(Class<?> clazz, String fieldName) {
        try {
            //noinspection unchecked
            return (T) FieldUtils.readStaticField(clazz, fieldName, true);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T invokeMethod(Object object, String methodName, Object... args) {
        try {
            //noinspection unchecked
            return (T) MethodUtils.invokeMethod(object, true, methodName, args);
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
