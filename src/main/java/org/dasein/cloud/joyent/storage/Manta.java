package org.dasein.cloud.joyent.storage;

import com.google.api.client.http.HttpResponseException;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaUtils;
import com.joyent.manta.exception.MantaCryptoException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.BasicConfigurator;
import org.dasein.cloud.*;
import org.dasein.cloud.examples.ProviderLoader;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.joyent.JoyentException;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.util.uom.storage.*;
import org.dasein.util.uom.storage.Byte;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * @author ilya.drabenia
 */
public class Manta implements BlobStoreSupport {
    private CloudProvider provider;

    public Manta(CloudProvider provider) {
        this.provider = provider;
    }

    @Override
    public boolean allowsNestedBuckets() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean allowsRootObjects() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean allowsPublicSharing() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearBucket(@Nonnull String bucket) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public Blob createBucket(@Nonnull String bucket, boolean findFreeName) throws InternalException, CloudException {
        try {
            CloudProvider provider = new ProviderLoader().getConfiguredProvider();
            provider.connect(null);
        } catch (Exception ex) {

        }
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public FileTransfer download(@Nullable String bucket, @Nonnull String objectName, @Nonnull File toFile) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean exists(@Nonnull String bucket) throws InternalException, CloudException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Blob getBucket(@Nonnull String bucketName) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Blob getObject(@Nullable String bucketName, @Nonnull String objectName) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nullable
    @Override
    public Storage<Byte> getObjectSize(@Nullable String bucketName, @Nullable String objectName) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMaxBuckets() throws CloudException, InternalException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Storage<Byte> getMaxObjectSize() throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMaxObjectsPerBucket() throws CloudException, InternalException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public NameRules getBucketNameRules() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public NameRules getObjectNameRules() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public String getProviderTermForBucket(@Nonnull Locale locale) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public String getProviderTermForObject(@Nonnull Locale locale) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isPublic(@Nullable String bucket, @Nullable String object) throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public Iterable<Blob> list(@Nullable String bucket) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void makePublic(@Nonnull String bucket) throws InternalException, CloudException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void makePublic(@Nullable String bucket, @Nonnull String object) throws InternalException, CloudException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void move(@Nullable String fromBucket, @Nullable String objectName, @Nullable String toBucket) throws InternalException, CloudException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeBucket(@Nonnull String bucket) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeObject(@Nullable String bucket, @Nonnull String object) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public String renameBucket(@Nonnull String oldName, @Nonnull String newName, boolean findFreeName) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void renameObject(@Nullable String bucket, @Nonnull String oldName, @Nonnull String newName) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    private static final String URL = "https://us-east.manta.joyent.com";
    private static final String LOGIN = "altoros2";
    private static final String KEY_PATH = "src/test/java/data/id_rsa";
    private static final String KEY_FINGERPRINT = "58:96:8b:6a:6a:1d:93:0a:6d:fc:fb:ef:8d:c2:00:a4";
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";
    private static final String TEST_FILE = "src/test/java/data/Master-Yoda.jpg";
    private static final String TEST_DIR_PATH = "/altoros2/stor/";// + UUID.randomUUID().toString() + "/";

    @Nonnull
    @Override
    public Blob upload(@Nonnull File sourceFile, @Nullable String bucket, @Nonnull String objectName)
            throws CloudException, InternalException {
        BasicConfigurator.configure();
        try {
            ProviderContext context = provider.getContext();

            final String LOGIN = context.getAccountNumber();
            final String URL = (String) context.getCustomProperties().get("STORAGE_URL");
            final String KEY_PATH = (String) context.getCustomProperties().get("KEY_PATH");
            final String KEY_FINGERPRINT = (String) context.getCustomProperties().get("KEY_FINGERPRINT");

            MantaClient client = MantaClient.newInstance(URL, LOGIN, KEY_PATH, KEY_FINGERPRINT);
            client.putDirectory(TEST_DIR_PATH, null);

            MantaObject mantaObject = new MantaObject(objectName);
            InputStream is = new FileInputStream(sourceFile);
            mantaObject.setDataInputStream(is);
            client.put(mantaObject);

            return Blob.getInstance(null, null, null, new Date().getTime());
        } catch (IOException ex) {
            throw new CloudException(ex);
        } catch (MantaCryptoException ex) {
            throw new CloudException(ex);
        }
    }

    @Nonnull
    @Override
    public String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }
}
