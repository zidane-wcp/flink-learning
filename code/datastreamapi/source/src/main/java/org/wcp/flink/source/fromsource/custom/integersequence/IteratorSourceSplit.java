package org.wcp.flink.source.fromsource.custom.integersequence;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Iterator;

@Public
public interface IteratorSourceSplit<E, IterT extends Iterator<E>> extends SourceSplit {
    IterT getIterator();

    IteratorSourceSplit<E, IterT> getUpdatedSplitForIterator(IterT var1);
}
