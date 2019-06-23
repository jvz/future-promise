package org.musigma.util.concurrent;

import java.util.concurrent.Callable;

class BatchingSchedulers {
    static final int SYNC_PRE_BATCH_DEPTH = 16;
    static final int RUN_LIMIT = 1024;
    static final BlockContext MISSING_PARENT_BLOCK_CONTEXT = new BlockContext() {
        @Override
        public <T> T blockOn(final Callable<T> thunk) throws Exception {
            throw new IllegalStateException("missing parent block context");
        }
    };
}
