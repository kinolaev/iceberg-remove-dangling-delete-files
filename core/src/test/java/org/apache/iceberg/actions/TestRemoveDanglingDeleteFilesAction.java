/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.actions;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DeleteFileSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestRemoveDanglingDeleteFilesAction {
    private static final Namespace NAMESPACE = Namespace.of("namespace");
    private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, "table");
    private static final Schema SCHEMA = new Schema(
            List.of(
                    required(1, "id", Types.IntegerType.get()),
                    required(2, "partition", Types.IntegerType.get()),
                    required(3, "snapshotId", Types.IntegerType.get())),
            Set.of(1));

    private static final InMemoryCatalog catalog = new InMemoryCatalog();
    private final DeleteFileSet danglingDeletes = DeleteFileSet.create();
    private Table table = null;

    @BeforeAll
    public static void setupCatalog() {
        catalog.initialize(null, Map.of());
        catalog.createNamespace(NAMESPACE);
    }

    @BeforeEach
    public void setupTable() {
        table = catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned());
    }

    @AfterEach
    public void cleanupTable() {
        catalog.dropTable(TABLE_ID, true);
        danglingDeletes.clear();
    }

    @AfterAll
    public static void cleanupCatalog() throws IOException {
        catalog.dropNamespace(NAMESPACE);
        catalog.close();
    }

    @Test
    void testUnpartitionedTableInsert() {
        singleRowDelta(1, 1, 0).commit();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(danglingDeletes::add);
        singleRowDelta(2, 1, 1).commit();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(danglingDeletes::add);
        assertDanglingDeleteFiles();
    }

    @Test
    void testUnpartitionedTableUpdate() {
        singleRowDelta(1, 1, 0).commit();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(danglingDeletes::add);
        singleRowDelta(1, 1, 1).commit();
        assertDanglingDeleteFiles();
    }

    @Test
    void testPartitionedTableInsertSamePartition() {
        table.updateSpec().addField("partition").commit();
        singleRowDelta(1, 1, 0).commit();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(danglingDeletes::add);
        singleRowDelta(2, 1, 1).commit();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(danglingDeletes::add);
        assertDanglingDeleteFiles();
    }

    @Test
    void testPartitionedTableUpdateSamePartition() {
        table.updateSpec().addField("partition").commit();
        singleRowDelta(1, 1, 0).commit();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(danglingDeletes::add);
        singleRowDelta(1, 1, 1).commit();
        assertDanglingDeleteFiles();
    }

    @Test
    void testPartitionedTableUpdateDifferentPartition() {
        table.updateSpec().addField("partition").commit();
        singleRowDelta(1, 1, 0).commit();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(danglingDeletes::add);
        singleRowDelta(1, 2, 1).commit();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(danglingDeletes::add);
        assertDanglingDeleteFiles();
    }

    @Test
    void testPartitionedTableInsertGlobalEqualityDelete() {
        table.updateSpec().addField("partition").commit();
        singleRowDelta(1, 1, 0).commit();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(danglingDeletes::add);
        singleRowDelta(2, 1, 1, PartitionSpec.unpartitioned()).commit();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(danglingDeletes::add);
        assertDanglingDeleteFiles();
    }

    @Test
    void testPartitionedTableUpdateGlobalEqualityDelete() {
        table.updateSpec().addField("partition").commit();
        singleRowDelta(1, 1, 0).commit();
        table.currentSnapshot().addedDeleteFiles(table.io()).forEach(danglingDeletes::add);
        singleRowDelta(1, 2, 1, PartitionSpec.unpartitioned()).commit();
        assertDanglingDeleteFiles();
    }

    void assertDanglingDeleteFiles() {
        RemoveDanglingDeleteFiles.Result result = new RemoveDanglingDeleteFilesAction(table).execute();
        assertThat(result.removedDeleteFiles()).isEqualTo(danglingDeletes);

        Snapshot snapshot = table.currentSnapshot();
        assertThat(snapshot.addedDataFiles(table.io())).isEmpty();
        assertThat(snapshot.removedDataFiles(table.io())).isEmpty();
        assertThat(snapshot.addedDeleteFiles(table.io())).isEmpty();
        assertThat(DeleteFileSet.of(snapshot.removedDeleteFiles(table.io()))).isEqualTo(danglingDeletes);
    }

    RowDelta singleRowDelta(int id, int partition, int snapshotId) {
        return singleRowDelta(id, partition, snapshotId, table.spec());
    }
    RowDelta singleRowDelta(int id, int partition, int snapshotId, PartitionSpec deleteSpec) {
        PartitionSpec spec = table.spec();
        PartitionData partitionData = null;
        if (spec.isPartitioned()) {
            partitionData = new PartitionData(spec.partitionType());
            partitionData.set(0, partition);
        }
        Map<Integer, ByteBuffer> bounds = Map.of(
                1, Conversions.toByteBuffer(Types.IntegerType.get(), id),
                2, Conversions.toByteBuffer(Types.IntegerType.get(), partition),
                3, Conversions.toByteBuffer(Types.IntegerType.get(), snapshotId));
        Metrics metrics = new Metrics(
                1L,
                Map.of(1, 4L, 2, 4L, 3, 4L), // column sizes
                Map.of(1, 1L, 2, 1L, 3, 4L), // value counts
                Map.of(1, 0L, 2, 0L, 3, 0L), // null value counts
                Map.of(1, 0L, 2, 0L, 3, 0L), // nan value counts
                bounds, // lower bounds
                bounds  // upper bounds
        );
        DataFile dataFile = DataFiles.builder(spec)
                .withPath("/%s-data.parquet".formatted(snapshotId))
                .withPartition(partitionData)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .withMetrics(metrics)
                .build();
        DeleteFile deleteFile = FileMetadata.deleteFileBuilder(deleteSpec)
                .ofEqualityDeletes(1)
                .withPath("/%s-delete.parquet".formatted(snapshotId))
                .withPartition(partitionData)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .withMetrics(metrics)
                .build();
        return table.newRowDelta().addRows(dataFile).addDeletes(deleteFile);
    }
}
