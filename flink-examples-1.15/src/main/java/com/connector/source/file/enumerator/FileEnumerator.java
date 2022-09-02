package com.connector.source.file.enumerator;

import com.connector.source.file.split.FileSourceSplit;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.Collection;

/**
 * 为指定路径下的相关文件生成所有文件拆分
 */
public interface FileEnumerator {
    /**
     * Generates all file splits for the relevant files under the given paths. The {@code
     * minDesiredSplits} is an optional hint indicating how many splits would be necessary to
     * exploit parallelism properly.
     */
    Collection<FileSourceSplit> enumerateSplits(Path[] paths,int minDesiredSplits) throws IOException;
}
