package com.connector.source.file.enumerator.assigner;

import com.connector.source.file.split.FileSourceSplit;

import java.util.Collection;
import java.util.Optional;

public interface FileSplitAssigner {

    /**
     * 获取下一个Split
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers finished their current splits.
     */
    Optional<FileSourceSplit> getNext();

    /**
     * 向此分配器添加一组split。
     * 例如，当某些split处理失败并且需要重新添加split时，或者当发现新split时就会发生这种情况
     * @param splits
     */
    void addSplits(Collection<FileSourceSplit> splits);

    /**
     * 获取此分配器待处理的剩余split
     * @return
     */
    Collection<FileSourceSplit> remainingSplits();
}
