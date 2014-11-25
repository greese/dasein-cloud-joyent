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

import org.dasein.cloud.CloudException;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.examples.ProviderLoader;
import org.dasein.cloud.joyent.storage.util.MantaStorageTestUtils;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.cloud.test.DaseinTestManager;
import org.dasein.util.uom.storage.Byte;
import org.dasein.util.uom.storage.Storage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Date;

import static org.junit.Assert.*;

/**
 * Manta storage smoke test. Provides basic tests for Manta Storage.
 *
 * @author ilya.drabenia
 * @author anton.karavaev
 */
public class MantaStorageSmokeTest {
    private static final String SRC_FILE_PATH = "src/test/resources/data/master-yoda.txt";
    private static final String MANTA_DIR_PATH = "smokeAK/";
    private static final String MANTA_FILE_NAME = "smokeAK/master-yoda.txt";

    private static BlobStoreSupport storage;

    @BeforeClass
    public static void prepareMantaStore() throws Exception {
        CloudProvider provider = DaseinTestManager.constructProvider();
        storage = provider.getStorageServices().getOnlineStorageSupport();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        storage.removeBucket(MANTA_DIR_PATH);
    }

    @Test
    public void testFileUpload() throws Exception {
        Blob result = MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, MANTA_DIR_PATH, MANTA_FILE_NAME);

        assertNotNull(result);
    }

    @Test
    public void testFileDownload() throws Exception {
        MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, MANTA_DIR_PATH, MANTA_FILE_NAME);
        File toFile = File.createTempFile(String.valueOf(new Date().getTime()), "");

        FileTransfer fileTransfer = storage.download(MANTA_DIR_PATH, MANTA_FILE_NAME, toFile);
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
        MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, MANTA_DIR_PATH, MANTA_FILE_NAME);

        storage.removeObject(MANTA_DIR_PATH, MANTA_FILE_NAME);
    }

    @Test
    public void testFileRename() throws Exception {
        MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, MANTA_DIR_PATH, MANTA_FILE_NAME);

        storage.renameObject(MANTA_DIR_PATH, MANTA_FILE_NAME, "Master-Yoda-1.jpg");
    }

    @Test
    public void testIsPublicForPublicFile() throws Exception {
        final String PUBLIC_FILE_PATH = "/altoros2/public/1/";

        boolean isPublic = storage.isPublic(PUBLIC_FILE_PATH, MANTA_FILE_NAME);

        assertTrue("Public files must be stored in /{login}/public folder", isPublic);
    }

    @Test
    public void testIsPublicForNotPublicFiles() throws Exception {
        final String PRIVATE_FILE_PATH = "/altoros2/stor/1/master-yoda.txt";

        boolean isPublic = storage.isPublic(null, PRIVATE_FILE_PATH);

        assertFalse(isPublic);
    }

    @Test
    public void testGetMaxObjectSize() throws Exception {
        double size = storage.getMaxObjectSize().doubleValue();

        assertTrue("Files in Manta has unlimited size", size > 1000000);
    }

    @Test
    public void testIsSubscribed() throws Exception {
        boolean isSubscribed = storage.isSubscribed();

        assertTrue("For make tests on real cloud client must be subscribed", isSubscribed);
    }

    /**
     * Note that current Manta Java API does not support recursive directory creation (i.e. mmkdir -p command in Node.js SDK)
     *
     * @throws Exception
     */
    @Test
    public void testDirectoryCreate() throws Exception {
        Blob bucket = MantaStorageTestUtils.createBucket(storage, MANTA_DIR_PATH);

        assertNotNull(bucket);
        assertNull("Manta directory must not have an object name", bucket.getObjectName());
        assertNotNull("Bucket name must be presented for Manta directories", bucket.getBucketName());
        assertEquals(bucket.getBucketName(), MANTA_DIR_PATH);
        assertTrue("Manta directory must be a container", bucket.isContainer());
    }

    @Test
    public void testDirectoryList() throws Exception {
        MantaStorageTestUtils.createBucket(storage, MANTA_DIR_PATH);
        MantaStorageTestUtils.createBucket(storage, MANTA_DIR_PATH + "foo/");
        MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, MANTA_DIR_PATH, MANTA_FILE_NAME);
        Iterable<Blob> blobs = storage.list(MANTA_DIR_PATH);

        assertTrue(blobs.iterator().hasNext());
    }

    @Test
    public void testDirectoryClear() throws Exception {
        String path = MantaStorageTestUtils.createBucket(storage, MANTA_DIR_PATH).getBucketName();
        storage.clearBucket(path);

        assertNull(storage.getBucket(path));
    }

    @Test
    public void testDirectoryExists() throws Exception {
        MantaStorageTestUtils.createBucket(storage, MANTA_DIR_PATH);

        assertTrue(storage.exists(MANTA_DIR_PATH));
    }

    @Test
    public void testDirectoryRemove() throws Exception {
        String path = MANTA_DIR_PATH + "2/";
        MantaStorageTestUtils.createBucket(storage, MANTA_DIR_PATH);
        MantaStorageTestUtils.createBucket(storage, path);
        storage.removeBucket(path);
        assertNull(storage.getBucket(path));
    }

    @Test
    public void testGetBucket() throws Exception {
        MantaStorageTestUtils.createBucket(storage, MANTA_DIR_PATH);
        Blob bucket = storage.getBucket(MANTA_DIR_PATH);

        assertNotNull(bucket);
    }

    @Test
    public void testGetObject() throws Exception {
        MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, MANTA_DIR_PATH, MANTA_FILE_NAME);
        Blob blob = storage.getObject(MANTA_DIR_PATH, MANTA_FILE_NAME);

        assertNotNull(blob);
    }

    @Test
    public void testGetObjectSize() throws Exception {
        Blob blob = MantaStorageTestUtils.uploadTestFile(SRC_FILE_PATH, storage, MANTA_DIR_PATH, MANTA_FILE_NAME);
        Storage<org.dasein.util.uom.storage.Byte> size = blob.getSize();
        Storage<Byte> storageObjectSize = storage.getObjectSize(MANTA_DIR_PATH, MANTA_FILE_NAME);
        if (size != null && storageObjectSize != null) {
            assertTrue(size.getQuantity().equals(storageObjectSize.getQuantity()));
        }
    }

}
