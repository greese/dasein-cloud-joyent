/**
 * Copyright (C) 2009-2015 Dell, Inc
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

package org.dasein.cloud.joyent.storage.util;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.BlobStoreSupport;

import javax.annotation.Nonnull;
import java.io.File;

/**
 * @author anton.karavaev
 */
public class MantaStorageTestUtils {

    @Nonnull
    public static Blob uploadTestFile(@Nonnull String src,
                                      @Nonnull BlobStoreSupport storage,
                                      @Nonnull String mantaPath,
                                      @Nonnull String mantaFileName) throws Exception {
        File sourceFile = new File(src);
        return storage.upload(sourceFile, mantaPath, mantaFileName);
    }

    @Nonnull
    public static Blob createBucket(@Nonnull BlobStoreSupport storage,
                                    @Nonnull String name) throws InternalException, CloudException {
        return storage.createBucket(name, false);
    }
}
