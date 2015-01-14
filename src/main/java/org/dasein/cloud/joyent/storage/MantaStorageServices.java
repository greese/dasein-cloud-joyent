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
    private SmartDataCenter provider;

    public MantaStorageServices(SmartDataCenter provider) {
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
