package org.dasein.cloud.joyent.storage;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.joyent.SmartDataCenter;
import org.dasein.cloud.storage.AbstractStorageServices;
import org.dasein.cloud.storage.BlobStoreSupport;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author ilya.drabenia
 */
public class MantaStorageServices extends AbstractStorageServices {
    private static final Logger logger = SmartDataCenter.getLogger(MantaStorageServices.class, "std");
    private CloudProvider provider;

    public MantaStorageServices(CloudProvider provider) {
        this.provider = provider;
    }

    @Nullable
    @Override
    public BlobStoreSupport getOnlineStorageSupport() {
        BlobStoreSupport storeSupport = null;
        try {
            storeSupport = new Manta(provider);
        } catch (IOException e) {
            logger.error("Could not initialize Manta Storage", e);
        } catch (CloudException e) {
            logger.error("Could not initialize Manta Storage", e);
        }
        return storeSupport;
    }
}
