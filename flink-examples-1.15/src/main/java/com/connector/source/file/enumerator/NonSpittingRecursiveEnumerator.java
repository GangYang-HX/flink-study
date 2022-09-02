package com.connector.source.file.enumerator;

import com.connector.source.file.split.FileSourceSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class NonSpittingRecursiveEnumerator implements FileEnumerator{

    private final char[] currentId = "0000000000000".toCharArray();

    @Override
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits) throws IOException {
        final ArrayList<FileSourceSplit> splits = new ArrayList<>();
        for (Path path:paths){
            final FileSystem fs = path.getFileSystem();
            final FileStatus status = fs.getFileStatus(path);
            addSplitsForPath(status,fs,splits);
        }
        return splits;
    }

    private void addSplitsForPath(FileStatus status, FileSystem fs, ArrayList<FileSourceSplit> splits) throws IOException{
        if (!status.isDir()){
            splits.add(new FileSourceSplit(getNextId(),status.getPath(),0,status.getLen()));
        }
    }

    public final String getNextId() {
        incrementCharArrayByOne(currentId,currentId.length-1);
        return new String(currentId);
    }

    private void incrementCharArrayByOne(char[] currentId, int pos) {
        char c = currentId[pos];
        c++;
        if (c>'9'){
            c = '0';
            incrementCharArrayByOne(currentId,pos-1);
        }
        currentId[pos] = c;
    }

}
