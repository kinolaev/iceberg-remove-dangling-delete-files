package org.apache.iceberg.actions;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.DeleteFileSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

public class RemoveDanglingDeleteFilesAction
        extends BaseSnapshotUpdateAction<RemoveDanglingDeleteFiles, RemoveDanglingDeleteFiles.Result>
        implements RemoveDanglingDeleteFiles {

    private static final Logger LOG = LoggerFactory.getLogger(RemoveDanglingDeleteFilesAction.class);

    private final Table table;
    private String branch = SnapshotRef.MAIN_BRANCH;

    public RemoveDanglingDeleteFilesAction(Table table) {
        this.table = table;
    }

    @Override
    protected RemoveDanglingDeleteFiles self() { return this; }

    @Override
    protected Table table() { return table; }

    public RemoveDanglingDeleteFilesAction toBranch(String targetBranch) {
        if (targetBranch == null) throw new IllegalArgumentException("Invalid branch name: null");
        this.branch = targetBranch;
        return this;
    }

    @Override
    public Result execute() {
        Snapshot snapshot = table.snapshot(branch);
        if (snapshot == null) throw new IllegalArgumentException(
                "Cannot remove dangling delete files for branch %s: branch does not exist".formatted(branch));
        TableScan scan = table.newScan().useSnapshot(snapshot.snapshotId());

        DeleteFileSet deletes = DeleteFileSet.create();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            tasks.forEach(task -> { deletes.addAll(task.deletes()); });
        } catch (IOException e) {
            LOG.error("Plan files error", e);
        }

        DeleteFileSet danglingDeletes = DeleteFileSet.create();
        snapshot.deleteManifests(table.io()).forEach(m -> {
            ManifestFiles.readDeleteManifest(m, table.io(), table.specs()).forEach(deleteFile -> {
                if (!deletes.contains(deleteFile)) {
                    danglingDeletes.add(deleteFile);
                }
            });
        });

        if (!danglingDeletes.isEmpty()) {
            RewriteFiles rewriteFiles = table.newRewrite().validateFromSnapshot(snapshot.snapshotId());
            for (DeleteFile deleteFile : danglingDeletes) {
                LOG.debug("Removing dangling delete file {}", deleteFile.location());
                rewriteFiles.deleteFile(deleteFile);
            }
            commit(rewriteFiles.toBranch(branch));
        }

        return ImmutableRemoveDanglingDeleteFiles.Result.builder()
                .removedDeleteFiles(danglingDeletes)
                .build();
    }
}
