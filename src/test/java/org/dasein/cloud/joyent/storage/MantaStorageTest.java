package org.dasein.cloud.joyent.storage;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.examples.ProviderLoader;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.BlobStoreSupport;
import static org.junit.Assert.*;

import org.dasein.cloud.storage.FileTransfer;
import org.junit.*;
import org.junit.rules.ExpectedException;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Date;

/**
 * @author ilya.drabenia
 * @author anton.karavaev
 */
public class MantaStorageTest {
    private static final String SRC_FILE_PATH = "src/test/resources/data/Master-Yoda.jpg";
    private static final String MANTA_DIR_PATH = "/altoros2/stor/1/";
    private static final String MANTA_FILE_NAME = "Master-Yoda.jpg";
    private static final String MANTA_FILE_PATH = MANTA_DIR_PATH + MANTA_FILE_NAME;

    private static BlobStoreSupport storage;

    @BeforeClass
    public static void prepareMantaStore() throws Exception {
        CloudProvider provider = new ProviderLoader().getConfiguredProvider();
        storage = provider.getStorageServices().getOnlineStorageSupport();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        storage.removeBucket(MANTA_DIR_PATH);
    }

    private Blob uploadTestFile() throws Exception {
        File sourceFile = new File(SRC_FILE_PATH);
        return storage.upload(sourceFile, MANTA_DIR_PATH, MANTA_FILE_NAME);
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
    public void testFileUpload() throws Exception {
        Blob result = uploadTestFile();

        assertNotNull(result);
    }

    @Test
    public void testFileDownload() throws Exception {
        uploadTestFile();
        File toFile = File.createTempFile(String.valueOf(new Date().getTime()), "");

        FileTransfer fileTransfer = storage.download(null, MANTA_FILE_PATH, toFile);
        waitUntilFileDownloaded(fileTransfer);

        assertThatFileSuccessfullyDownloaded(fileTransfer);
        assertEquals(toFile.length(), 892907);
    }

    @Test
    public void testFileRemove() throws Exception {
        uploadTestFile();

        storage.removeObject(null, MANTA_FILE_PATH);
    }

    @Test
    public void testFileRename() throws Exception {
        uploadTestFile();

        storage.renameObject(null, MANTA_FILE_PATH, "/altoros2/stor/1/Master-Yoda-1.jpg");
    }

    @Test
    public void testIsPublicForPublicFile() throws Exception {
        final String PUBLIC_FILE_PATH = "/altoros2/public/1/";

        boolean isPublic = storage.isPublic(PUBLIC_FILE_PATH, MANTA_FILE_NAME);

        assertTrue("Public files must be stored in /{login}/public folder", isPublic);
    }

    @Test
    public void testIsPublicForNotPublicFiles() throws Exception {
        final String PRIVATE_FILE_PATH = "/altoros2/stor/1/Master-Yoda.jpg";

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
        Blob bucket = createBucket(MANTA_DIR_PATH);

        assertNotNull(bucket);
        assertNull("Manta directory must not have an object name", bucket.getObjectName());
        assertNotNull("Bucket name must be presented for Manta directories", bucket.getBucketName());
        assertEquals(bucket.getBucketName(), MANTA_DIR_PATH);
        assertTrue("Manta directory must be a container", bucket.isContainer());
    }

    @Test
    public void testDirectoryList() throws Exception {
        createBucket(MANTA_DIR_PATH);
        createBucket(MANTA_DIR_PATH + "foo/");
        uploadTestFile();
        Iterable<Blob> blobs = storage.list(MANTA_DIR_PATH);

        assertTrue(blobs.iterator().hasNext());
    }

    @Test
    public void testDirectoryClear() throws Exception {
        Blob parent = createBucket(MANTA_DIR_PATH);
        storage.clearBucket(parent.getBucketName());
        Iterable<Blob> blobs = storage.list(parent.getBucketName());

        assertFalse(blobs.iterator().hasNext());
    }

    @Test
    public void testDirectoryExists() throws Exception {
        createBucket(MANTA_DIR_PATH);

        assertTrue(storage.exists(MANTA_DIR_PATH));
    }

    @Nonnull
    private Blob createBucket(@Nonnull String name) throws InternalException, CloudException {
        return storage.createBucket(name, false);
    }

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test(expected = CloudException.class)
    public void testDirectoryRemove() throws Exception {
        String path = MANTA_DIR_PATH + "2/";
        createBucket(MANTA_DIR_PATH);
        createBucket(path);
        storage.removeBucket(path);
        storage.getBucket(path);
    }

    @Test
    public void testGetBucket() throws Exception {
        createBucket(MANTA_DIR_PATH);
        Blob bucket = storage.getBucket(MANTA_DIR_PATH);

        assertNotNull(bucket);
    }

    @Test
    public void testGetObject() throws Exception {
        uploadTestFile();
        Blob blob = storage.getObject(MANTA_DIR_PATH, MANTA_FILE_NAME);

        assertNotNull(blob);
    }

    // TODO: Test is ignored until Measured.convertTo will work correct
    @Ignore
    @Test
    public void testGetObjectSize() throws Exception {
        Blob blob = uploadTestFile();
        // Measured.convertTo returns double. Test is disabled
        assertTrue(blob.getSize().getQuantity().equals(storage.getObjectSize(MANTA_DIR_PATH, MANTA_FILE_NAME).getQuantity()));
    }
}
