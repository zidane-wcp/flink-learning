package org.wcp.flink.source.fromsource.custom.integersequence;


import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

@Public
public class IteratorSourceEnumerator<SplitT extends IteratorSourceSplit<?, ?>> implements SplitEnumerator<SplitT, Collection<SplitT>> {
    private final SplitEnumeratorContext<SplitT> context;
    private final Queue<SplitT> remainingSplits;

    public IteratorSourceEnumerator(SplitEnumeratorContext<SplitT> context, Collection<SplitT> splits) {
        this.context = (SplitEnumeratorContext) Preconditions.checkNotNull(context);
        this.remainingSplits = new ArrayDeque(splits);
    }

    public void start() {
    }

    public void close() {
    }

    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        SplitT nextSplit = (SplitT) this.remainingSplits.poll();
        if (nextSplit != null) {
            this.context.assignSplit(nextSplit, subtaskId);
        } else {
            this.context.signalNoMoreSplits(subtaskId);
        }

    }

    public void addSplitsBack(List<SplitT> splits, int subtaskId) {
        this.remainingSplits.addAll(splits);
    }

    public Collection<SplitT> snapshotState(long checkpointId) throws Exception {
        return this.remainingSplits;
    }

    public void addReader(int subtaskId) {
    }
}
