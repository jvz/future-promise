package org.musigma.util.concurrent;

import org.apiguardian.api.API;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

// TODO: add equivalent marker interface?
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@API(status = API.Status.EXPERIMENTAL)
public @interface Batchable {
}
