package com.connector.source.file.enumerator;

import com.connector.source.file.split.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * SplitEnumerator checkpoint数据，保存了未分配的分片以及处理过的文件列表
 */
public class PendingSplitsCheckpoint {
    private final Collection<FileSourceSplit> splits;

    private final Collection<Path> alreadyProcessedPaths;

    @Nullable byte[] serializedFormCache;

    protected PendingSplitsCheckpoint(Collection<FileSourceSplit> splits,Collection<Path> alreadyProcessedPaths){
        this.splits = Collections.unmodifiableCollection(splits);
        this.alreadyProcessedPaths = Collections.unmodifiableCollection(alreadyProcessedPaths);
    }

    public Collection<FileSourceSplit> getSplits() {
        return splits;
    }

    public Collection<Path> getAlreadyProcessedPaths() {
        return alreadyProcessedPaths;
    }

    @Override
    public String toString() {
        return "PendingSplitsCheckpoint{"
                + "splits="
                + splits
                + ", alreadyProcessedPaths="
                + alreadyProcessedPaths
                + '}';
    }

    public static PendingSplitsCheckpoint fromCollectionSnapshot(final Collection<FileSourceSplit> splits){
        Preconditions.checkNotNull(splits);
        final Collection<FileSourceSplit> copy = new ArrayList<>(splits);
        return new PendingSplitsCheckpoint(copy,Collections.emptySet());
    }

    public static PendingSplitsCheckpoint fromCollectionSnapshot(final Collection<FileSourceSplit> splits,final Collection<Path> alreadyProcessedPaths){
        Preconditions.checkNotNull(splits);
        final Collection<FileSourceSplit> splitCopy = new ArrayList<>(splits);
        final Collection<Path> pathsCopy = new ArrayList<>(alreadyProcessedPaths);
        return new PendingSplitsCheckpoint(splitCopy,pathsCopy);
    }

    static PendingSplitsCheckpoint reusingCollection(final Collection<FileSourceSplit> splits,final Collection<Path> alreadyProcessedPaths){
        return new PendingSplitsCheckpoint(splits,alreadyProcessedPaths);
    }

}
