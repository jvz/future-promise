package org.musigma.util.control;

public abstract class ControlThrowable extends Throwable {

    protected ControlThrowable() {
        this(null);
    }

    protected ControlThrowable(final String message) {
        super(message, null, false, false);
    }

}
