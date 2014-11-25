/**
 * Copyright (C) 2009-2014 Dell, Inc
 * See annotations for authorship information
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.joyent.storage;

import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.joyent.storage.util.MantaStorageTestUtils;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.cloud.test.DaseinTestManager;
import org.dasein.util.uom.storage.Byte;
import org.dasein.util.uom.storage.Storage;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * Sophisticated tests with various valid data parameters.
 *
 * @author ilya.drabenia
 * @author anton.karavaev
 */
@RunWith(Parameterized.class)
public class MantaStorageTest {
    private static final String SRC_FILE_PATH = "src/test/resources/data/master-yoda.txt";

    private static BlobStoreSupport storage;

    @Parameterized.Parameters
    public static Collection<Object[]> validData() {
        return Arrays.asList(new Object[][]{
                {"testAK/2/", "0-master-yoda.txt"},
                {"testAK/2", "1-master-yoda.txt"},
                {"testAK/2", "testAK/2/3-master-yoda.txt"},
                {"testAK/2/", "testAK/2/4-master-yoda.txt"},
                {"testAK/2/    ", "testAK/2/5-master-yoda.txt"},

        });
    }

    @Parameterized.Parameter
    public String dir;

    @Parameterized.Parameter(value = 1)
    public String fileName;

    @BeforeClass
    public static void prepareMantaStore() throws Exception {
        CloudProvider provider = DaseinTestManager.constructProvider();
        storage = provider.getStorageServices().getOnlineStorageSupport();
    }

    @After
    public void tearDown() throws Exception {
        if (storage.exists(dir)) {
            storage.removeBucket(dir);
        }
    }

    @Test
    public void testFileUpload() throws Exception {
        Blob result = MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, dir, fileName);

        assertNotNull(result);
        assertTrue(storage.exists(dir));
        storage.getObject(dir, fileName);
    }

    @Test
    public void testFileDownload() throws Exception {
        MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, dir, fileName);
        File toFile = File.createTempFile(String.valueOf(new Date().getTime()), "");

        FileTransfer fileTransfer = storage.download(dir, fileName, toFile);
        waitUntilFileDownloaded(fileTransfer);

        assertThatFileSuccessfullyDownloaded(fileTransfer);
        assertEquals(toFile.length(), 16);
    }

    private void assertThatFileSuccessfullyDownloaded(FileTransfer fileTransfer) {
        Throwable error = fileTransfer.getTransferError();

        assertNull("After file download Task object must not contains errors", error);
    }

    private void waitUntilFileDownloaded(FileTransfer fileTransfer) throws InterruptedException {
        while (!fileTransfer.isComplete()) {
            synchronized (fileTransfer) {
                fileTransfer.wait(10000L);
            }
        }
    }

    @Test
    public void testFileRemove() throws Exception {
        MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, dir, fileName);

        storage.removeObject(dir, fileName);
    }

    @Test
    public void testFileRename() throws Exception {
        MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, dir, fileName);

        storage.renameObject(dir, fileName, "Master-Yoda-1.jpg");
    }


    /**
     * Note that current Manta Java API does not support recursive directory creation (i.e. mmkdir -p command in Node.js SDK)
     *
     * @throws Exception
     */
    @Test
    public void testDirectoryCreate() throws Exception {
        Blob bucket = MantaStorageTestUtils.createBucket(storage, dir);

        assertNotNull(bucket);
        assertNull("Manta directory must not have an object name", bucket.getObjectName());
        assertNotNull("Bucket name must be presented for Manta directories", bucket.getBucketName());
        assertEquals(bucket.getBucketName(), dir);
        assertTrue("Manta directory must be a container", bucket.isContainer());
    }

    @Test
    public void testDirectoryList() throws Exception {
        MantaStorageTestUtils.createBucket(storage, dir);
        MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, dir, fileName);
        Iterable<Blob> blobs = storage.list(dir);

        assertTrue(blobs.iterator().hasNext());
    }

    @Test
    public void testDirectoryClear() throws Exception {
        String path = MantaStorageTestUtils.createBucket(storage, dir).getBucketName();
        storage.clearBucket(path);

        assertNull(storage.getBucket(path));
    }

    @Test
    public void testDirectoryRemove() throws Exception {
        String path = dir + "2/";
        MantaStorageTestUtils.createBucket(storage, dir);
        MantaStorageTestUtils.createBucket(storage, path);
        storage.removeBucket(path);
        assertNull(storage.getBucket(path));
    }

    @Test
    public void testGetBucket() throws Exception {
        MantaStorageTestUtils.createBucket(storage, dir);
        Blob bucket = storage.getBucket(dir);

        assertNotNull(bucket);
    }

    @Test
    public void testGetObject() throws Exception {
        MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, dir, fileName);
        Blob blob = storage.getObject(dir, fileName);

        assertNotNull(blob);
    }

    @Test
    public void testGetObjectSize() throws Exception {
        Blob blob = MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, dir, fileName);
        Storage<Byte> size = blob.getSize();
        Storage<Byte> storageObjectSize = storage.getObjectSize(dir, fileName);
        if (size != null && storageObjectSize != null) {
            assertTrue(size.getQuantity().equals(storageObjectSize.getQuantity()));
        }
    }
}
