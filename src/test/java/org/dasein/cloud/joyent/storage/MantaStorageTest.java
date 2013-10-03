package org.dasein.cloud.joyent.storage;

import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.examples.ProviderLoader;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.BlobStoreSupport;
import static org.junit.Assert.*;

import org.dasein.cloud.storage.FileTransfer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Date;

/**
 * @author ilya.drabenia
 */
public class MantaStorageTest {
    private static final String SRC_FILE_PATH = "src/test/resources/data/Master-Yoda.jpg";
    private static final String MANTA_FILE_PATH = "/altoros2/stor/1/Master-Yoda.jpg";

    private static BlobStoreSupport storage;

    @BeforeClass
    public static void prepareMantaStore() throws Exception {
        CloudProvider provider = new ProviderLoader().getConfiguredProvider();
        storage = provider.getStorageServices().getOnlineStorageSupport();
    }

    private Blob uploadTestFile() throws Exception {
        File sourceFile = new File(SRC_FILE_PATH);
        return storage.upload(sourceFile, null, MANTA_FILE_PATH);
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
        final String PUBLIC_FILE_PATH = "/altoros2/public/1/Master-Yoda.jpg";

        boolean isPublic = storage.isPublic(null, PUBLIC_FILE_PATH);

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

}
