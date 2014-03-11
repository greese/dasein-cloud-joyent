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
