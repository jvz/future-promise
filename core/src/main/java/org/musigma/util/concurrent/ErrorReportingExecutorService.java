package org.musigma.util.concurrent;

import java.util.concurrent.ExecutorService;

public interface ErrorReportingExecutorService extends ExecutorService {
    void reportFailure(final Throwable error);
}
