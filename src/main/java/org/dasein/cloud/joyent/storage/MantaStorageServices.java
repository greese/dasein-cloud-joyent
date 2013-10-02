package org.dasein.cloud.joyent.storage;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.joyent.SmartDataCenter;
import org.dasein.cloud.storage.AbstractStorageServices;
import org.dasein.cloud.storage.BlobStoreSupport;

import javax.annotation.Nullable;

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
        try {
            return new Manta(provider);
        } catch (Exception ex) {
            logger.error("Could not initialize Manta Storage", ex);
            return null;
        }
    }
}
