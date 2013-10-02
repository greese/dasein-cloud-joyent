package org.dasein.cloud.joyent.storage;

import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.storage.AbstractStorageServices;
import org.dasein.cloud.storage.BlobStoreSupport;

import javax.annotation.Nullable;

/**
 * @author ilya.drabenia
 */
public class MantaStorageServices extends AbstractStorageServices {
    private CloudProvider provider;

    public MantaStorageServices(CloudProvider provider) {
        this.provider = provider;
    }

    @Nullable
    @Override
    public BlobStoreSupport getOnlineStorageSupport() {
        return new Manta(provider);
    }
}
