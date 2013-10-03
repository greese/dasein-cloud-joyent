package org.dasein.cloud.joyent.storage;

import org.dasein.cloud.CloudException;
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
    private static BlobStoreSupport storage;

    @BeforeClass
    public static void initializeMantaStore() throws Exception {
        CloudProvider provider = new ProviderLoader().getConfiguredProvider();
        storage = provider.getStorageServices().getOnlineStorageSupport();
    }

    private Blob createFile() throws Exception {
        return storage.upload(new File("src/test/resources/data/Master-Yoda.jpg"), null,
                "/altoros2/stor/1/Master-Yoda.jpg");
    }

    private void assertThatFileSuccessfullyDownloaded(FileTransfer fileTransfer) {
        Throwable error = fileTransfer.getTransferError();
        assertTrue(error != null ? error.toString() : "", error == null);
    }

    @Test
    public void testFileUpload() throws Exception {
        Blob result = createFile();

        assertTrue(result != null);
    }

    @Test
    public void testFileDownload() throws Exception {
        createFile();

        FileTransfer fileTransfer = storage.download(null, "/altoros2/stor/1/Master-Yoda.jpg", File.createTempFile(
                String.valueOf(new Date().getTime()), ""));

        while (!fileTransfer.isComplete()) {
            synchronized (fileTransfer) {
                fileTransfer.wait(10000L);
            }
        }

        assertThatFileSuccessfullyDownloaded(fileTransfer);
    }

    @Test
    public void testFileRemove() throws Exception {
        createFile();

        storage.removeObject(null, "/altoros2/stor/1/Master-Yoda.jpg");
    }

    @Test
    public void testFileRename() throws Exception {
        createFile();

        storage.renameObject(null, "/altoros2/stor/1/Master-Yoda.jpg", "/altoros2/stor/1/Master-Yoda1.jpg");
    }

}
