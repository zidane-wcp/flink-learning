package org.wcp.flink.source.fromsource.custom.integersequence;


import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Public
public class IteratorSourceReader<E, IterT extends Iterator<E>, SplitT extends IteratorSourceSplit<E, IterT>> implements SourceReader<E, SplitT> {
    private final SourceReaderContext context;
    private CompletableFuture<Void> availability;
    @Nullable
    private IterT iterator;
    @Nullable
    private SplitT currentSplit;
    private final Queue<SplitT> remainingSplits;
    private boolean noMoreSplits;

    public IteratorSourceReader(SourceReaderContext context) {
        this.context = (SourceReaderContext) Preconditions.checkNotNull(context);
        this.availability = new CompletableFuture();
        this.remainingSplits = new ArrayDeque();
    }

    public void start() {
        if (this.remainingSplits.isEmpty()) {
            this.context.sendSplitRequest();
        }

    }

    public InputStatus pollNext(ReaderOutput<E> output) {
        if (this.iterator != null) {
            if (this.iterator.hasNext()) {
                output.collect(this.iterator.next());
                return InputStatus.MORE_AVAILABLE;
            }

            this.finishSplit();
        }

        return this.tryMoveToNextSplit();
    }

    private void finishSplit() {
        this.iterator = null;
        this.currentSplit = null;
        if (this.remainingSplits.isEmpty() && !this.noMoreSplits) {
            this.context.sendSplitRequest();
        }

    }

    private InputStatus tryMoveToNextSplit() {
        this.currentSplit = this.remainingSplits.poll();
        if (this.currentSplit != null) {
            this.iterator = this.currentSplit.getIterator();
            return InputStatus.MORE_AVAILABLE;
        } else if (this.noMoreSplits) {
            return InputStatus.END_OF_INPUT;
        } else {
            if (this.availability.isDone()) {
                this.availability = new CompletableFuture();
            }

            return InputStatus.NOTHING_AVAILABLE;
        }
    }

    public CompletableFuture<Void> isAvailable() {
        return this.availability;
    }

    public void addSplits(List<SplitT> splits) {
        this.remainingSplits.addAll(splits);
        this.availability.complete((Void) null);
    }

    public void notifyNoMoreSplits() {
        this.noMoreSplits = true;
        this.availability.complete(null);
    }

    public List<SplitT> snapshotState(long checkpointId) {
        if (this.currentSplit == null && this.remainingSplits.isEmpty()) {
            return Collections.emptyList();
        } else {
            ArrayList<SplitT> allSplits = new ArrayList<>(1 + this.remainingSplits.size());
            if (this.iterator != null && this.iterator.hasNext()) {
                assert this.currentSplit != null;

                SplitT inProgressSplit = (SplitT) this.currentSplit.getUpdatedSplitForIterator(this.iterator);
                allSplits.add(inProgressSplit);
            }

            allSplits.addAll(this.remainingSplits);
            return allSplits;
        }
    }

    public void close() throws Exception {
    }
}
