package org.dasein.cloud.joyent.manta;

import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.examples.ProviderLoader;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.BlobStoreSupport;
import static org.junit.Assert.*;
import org.junit.Test;

import java.io.File;

/**
 * @author ilya.drabenia
 */
public class MantaStorageTest {

    @Test
    public void testFileUpload() throws Exception {
        CloudProvider provider = new ProviderLoader().getConfiguredProvider();

        BlobStoreSupport storage = provider.getStorageServices().getOnlineStorageSupport();

        Blob result = storage.upload(new File("src/test/resources/data/Master-Yoda.jpg"), null, "Master-Yoda.jpg");

        assertTrue(result != null);
    }

}
