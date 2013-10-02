package org.dasein.cloud.joyent.manta;

import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.ProviderContext;
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

    @Test
    public void testFileUpload() throws Exception {
        Blob result = storage.upload(new File("src/test/resources/data/Master-Yoda.jpg"), null, "Master-Yoda.jpg");

        assertTrue(result != null);
    }

    @Test
    public void testFileDownload() throws Exception {
        FileTransfer fileTransfer = storage.download(null, "/altoros2/stor/Master-Yoda.jpg", File.createTempFile(
                String.valueOf(new Date().getTime()), ""));

        while (!fileTransfer.isComplete()) {
            synchronized (fileTransfer) {
                fileTransfer.wait(10000L);
            }
        }

        assertThatFileSuccessfullyDownloaded(fileTransfer);
    }

    private void assertThatFileSuccessfullyDownloaded(FileTransfer fileTransfer) {
        Throwable error = fileTransfer.getTransferError();
        assertTrue(error != null ? error.toString() : "", error == null);
    }

}
