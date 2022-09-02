package com.connector.source.file.enumerator;

import com.connector.source.file.enumerator.assigner.FileSplitAssigner;
import com.connector.source.file.enumerator.assigner.SimpleSplitAssigner;
import com.connector.source.file.split.FileSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 定期监测目录下的文件，生成数据分片，并分配给SourceReader。Flink内核提供了定时回调接口
 */
public class FileSourceEnumerator implements SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> {

    private static final Logger LOG = LoggerFactory.getLogger(FileSourceEnumerator.class);

    private final FileEnumerator enumerator; //用于生成Split
    private final FileSplitAssigner splitAssigner; //用于分配Split

    private final SplitEnumeratorContext<FileSourceSplit> context;
    private final Path[] paths;
    private final HashSet<Path> alreadyDiscoveredPaths;

    private final LinkedHashMap<Integer, String> readersAwaitingSplit;

    public FileSourceEnumerator(SplitEnumeratorContext<FileSourceSplit> context, Path[] paths, Collection<FileSourceSplit> splits, Collection<Path> alreadyDiscoveredPaths) {
        this.enumerator = new NonSpittingRecursiveEnumerator();
        this.splitAssigner = new SimpleSplitAssigner(Preconditions.checkNotNull(splits));
        this.context = context;
        this.paths = paths;
        this.alreadyDiscoveredPaths = new HashSet<>(alreadyDiscoveredPaths);
        this.readersAwaitingSplit = new LinkedHashMap<>();
    }

    @Override
    public void start() {
        /**
         * call方法包含四个参数。
         * 1.callable：可调用的调用
         * 2.handler: 处理可调用对象的返回值或抛出的异常的处理程序
         * 3.initialDelayMillis: 调用可调用对象的初始延迟，以毫秒为单位
         * 4.periodMillis:两次调用callable之间的时间间隔，以毫秒为单位
         *
         * 这个方法的意思就是延迟initialDelayMillis之后调用callable，然后把callable的结果传给handler，最后以periodMillis的频率重复这个过程
         */
        context.callAsync(() -> enumerator.enumerateSplits(paths, 1), this::processDiscoveredSplits, 2000, 1000);
    }

    @Override
    public void handleSplitRequest(int subTaskId, @Nullable String requestHostName) {
        readersAwaitingSplit.put(subTaskId, requestHostName);
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<FileSourceSplit> splits, int subTaskId) {
        LOG.info("File Source Enumerator adds splits back : {}", splits);
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int i) {
        //this source is purely laze-pull-based,nothing to do upon registration
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long l) throws Exception {
        final PendingSplitsCheckpoint checkpoint = PendingSplitsCheckpoint.fromCollectionSnapshot(splitAssigner.remainingSplits(), alreadyDiscoveredPaths);
        LOG.debug("Source checkpoint is {}", checkpoint);
        return checkpoint;
    }

    @Override
    public void close() throws IOException {

    }

    private void processDiscoveredSplits(Collection<FileSourceSplit> splits,Throwable error){
        if (error != null){
            LOG.error("Failed to enumerator files,{}",error);
            return;
        }
        final Collection<FileSourceSplit> newSplits = splits.stream().filter(split->alreadyDiscoveredPaths.add(split.path())).collect(Collectors.toList());
        splitAssigner.addSplits(newSplits);
        assignSplits();
    }

    private void assignSplits() {
        final Iterator<Map.Entry<Integer, String>> awaitingReader = readersAwaitingSplit.entrySet().iterator();
        while (awaitingReader.hasNext()) {
            final Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();
            //if the reader that requested another split has failed in the meantime,remove
            //it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting.getKey())) {
                awaitingReader.remove();
                continue;
            }
            final String hostName = nextAwaiting.getValue();
            final int awaitingSubTask = nextAwaiting.getKey();

            final Optional<FileSourceSplit> nextSplit = splitAssigner.getNext();
            if (nextSplit.isPresent()) {
                context.assignSplit(nextSplit.get(), awaitingSubTask);
                awaitingReader.remove();
            } else {
                break;
            }
        }
    }
}
