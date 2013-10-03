package org.dasein.cloud.joyent.storage;

import com.google.api.client.http.HttpResponseException;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.exception.MantaCryptoException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.joyent.SmartDataCenter;
import org.dasein.cloud.storage.Blob;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.storage.FileTransfer;
import org.dasein.util.uom.storage.*;
import org.dasein.util.uom.storage.Byte;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.Date;
import java.util.Locale;

/**
 * @author ilya.drabenia
 */
public class Manta implements BlobStoreSupport {
    private static final Logger logger = SmartDataCenter.getLogger(MantaStorageServices.class, "std");

    private CloudProvider provider;
    private final MantaClient mantaClient;

    public Manta(CloudProvider provider) throws IOException, CloudException {
        this.provider = provider;
        this.mantaClient = getClient();
    }

    private MantaClient getClient() throws CloudException, IOException {
        ProviderContext context = provider.getContext();

        final String LOGIN = context.getAccountNumber();
        final String URL = (String) context.getCustomProperties().get("STORAGE_URL");
        final String KEY_PATH = (String) context.getCustomProperties().get("KEY_PATH");
        final String KEY_FINGERPRINT = (String) context.getCustomProperties().get("KEY_FINGERPRINT");

        return MantaClient.newInstance(URL, LOGIN, KEY_PATH, KEY_FINGERPRINT);
    }

    /**
     * Manta does not support buckets
     * @throws CloudException
     * @throws InternalException
     * @return
     */
    @Override
    public boolean allowsNestedBuckets() throws CloudException, InternalException {
        return false;
    }

    /**
     * Manta does not support objects on root level
     * @throws CloudException
     * @throws InternalException
     * @return
     */
    @Override
    public boolean allowsRootObjects() throws CloudException, InternalException {
        return false;
    }

    /**
     * Manta allow public sharing using directory public
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public boolean allowsPublicSharing() throws CloudException, InternalException {
        return true;
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     * @param bucket
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public void clearBucket(@Nonnull String bucket) throws CloudException, InternalException {
        throw new UnsupportedOperationException("Manta does not have support of buckets");
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     * @param bucket
     * @param findFreeName
     * @return
     * @throws InternalException
     * @throws CloudException
     */
    @Nonnull
    @Override
    public Blob createBucket(@Nonnull String bucket, boolean findFreeName) throws InternalException, CloudException {
        throw new UnsupportedOperationException("Manta does not have support of buckets");
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     * @param bucket
     * @return
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public boolean exists(@Nonnull String bucket) throws InternalException, CloudException {
        throw new UnsupportedOperationException("Manta does not have support of buckets");
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     * @param bucketName
     * @return
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public Blob getBucket(@Nonnull String bucketName) throws InternalException, CloudException {
        throw new UnsupportedOperationException("Manta does not have support of buckets");
    }

    /**
     *
     * @param bucketName
     * @param objectName
     * @return
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public Blob getObject(@Nullable String bucketName, @Nonnull String objectName) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     *
     * @param bucketName
     * @param objectName
     * @return
     * @throws InternalException
     * @throws CloudException
     */
    @Nullable
    @Override
    public Storage<Byte> getObjectSize(@Nullable String bucketName, @Nullable String objectName)
            throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public int getMaxBuckets() throws CloudException, InternalException {
        throw new UnsupportedOperationException("Manta does not have support of buckets");
    }

    /**
     *
     * @return
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public Storage<Byte> getMaxObjectSize() throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public int getMaxObjectsPerBucket() throws CloudException, InternalException {
        throw new UnsupportedOperationException("Manta does not have support of buckets");
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Nonnull
    @Override
    public NameRules getBucketNameRules() throws CloudException, InternalException {
        throw new UnsupportedOperationException("Manta does not have support of buckets");
    }

    /**
     *
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Nonnull
    @Override
    public NameRules getObjectNameRules() throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     * @param locale
     * @return
     */
    @Nonnull
    @Override
    public String getProviderTermForBucket(@Nonnull Locale locale) {
        throw new UnsupportedOperationException("Manta does not have support of buckets");
    }

    /**
     *
     * @param locale
     * @return
     */
    @Nonnull
    @Override
    public String getProviderTermForObject(@Nonnull Locale locale) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     *
     * @param bucket
     * @param object
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public boolean isPublic(@Nullable String bucket, @Nullable String object) throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     *
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public Iterable<Blob> list(@Nullable String bucket) throws CloudException, InternalException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     * @param bucket
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public void makePublic(@Nonnull String bucket) throws InternalException, CloudException {
        throw new UnsupportedOperationException("Manta does not have support of buckets");
    }

    @Override
    public void makePublic(@Nullable String bucket, @Nonnull String object) throws InternalException, CloudException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     * @param fromBucket
     * @param objectName
     * @param toBucket
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public void move(@Nullable String fromBucket, @Nullable String objectName, @Nullable String toBucket) throws
            InternalException, CloudException {
        throw new UnsupportedOperationException("Manta does not have support of buckets");
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     * @param bucket
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public void removeBucket(@Nonnull String bucket) throws CloudException, InternalException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeObject(@Nullable String bucket, @Nonnull String object) throws CloudException, InternalException {
        try {
            mantaClient.delete(object);
        } catch (MantaCryptoException ex) {
            throw new CloudException(ex);
        } catch (IOException ex) {
            throw new CloudException(ex);
        }
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     * @param oldName
     * @param newName
     * @param findFreeName
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Nonnull
    @Override
    public String renameBucket(@Nonnull String oldName, @Nonnull String newName, boolean findFreeName) throws CloudException, InternalException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameObject(@Nullable String bucket, @Nonnull String oldName, @Nonnull String newName) throws CloudException, InternalException {
        try {
            mantaClient.putSnapLink(newName, oldName, null);
            mantaClient.delete(oldName);
        } catch (MantaCryptoException ex) {
            throw new CloudException(ex);
        } catch (IOException ex) {
            throw new CloudException(ex);
        }
    }

    /**
     * Method upload {@code sourceFile} as {@code objectName} to Manta.
     * @param sourceFile
     * @param bucket
     * @param objectName
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Nonnull
    @Override
    public Blob upload(@Nonnull File sourceFile, @Nullable String bucket, @Nonnull String objectName)
            throws CloudException, InternalException {
        try {
            return processFileUpload(sourceFile, objectName);
        } catch (IOException ex) {
            throw new CloudException(ex);
        } catch (MantaCryptoException ex) {
            throw new CloudException(ex);
        }
    }

    private Blob processFileUpload(@Nonnull File sourceFile, @Nonnull String objectName) throws IOException,
            MantaCryptoException {
        mantaClient.putDirectory(parseDirectoryName(objectName), null);

        MantaObject mantaObject = new MantaObject(objectName);
        mantaObject.setDataInputStream(new FileInputStream(sourceFile));
        mantaClient.put(mantaObject);

        return Blob.getInstance("", objectName, "", new Date().getTime());
    }

    private String parseDirectoryName(@Nonnull String objectName) {
        return objectName.substring(0, objectName.lastIndexOf('/') + 1);
    }

    private String parseFileName(@Nonnull String objectName) {
        return objectName.replaceAll("^/(.*)/", "");
    }

    /**
     * Method download file {@code objectName} from Manta to file {@code toFile}. Action occurs asynchronous.
     * @param bucket
     * @param objectName
     * @param toFile
     * @return
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public FileTransfer download(@Nullable String bucket, @Nonnull final String objectName, final @Nonnull File toFile)
            throws InternalException, CloudException {
        final FileTransfer fileTransfer = new FileTransfer();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    processDownloadAsync(fileTransfer, objectName, toFile);
                } catch (Exception ex) {
                    logger.error("Error on file download from Manta Storage", ex);
                    fileTransfer.complete(ex);
                }
            }
        }).start();

        return fileTransfer;
    }

    private void processDownloadAsync(FileTransfer fileTransfer, String objectName, File toFile) throws IOException,
            MantaCryptoException {
        synchronized (fileTransfer) {
            fileTransfer.setStartTime(new Date().getTime());
            fileTransfer.setPercentComplete(0);
        }

        MantaObject mantaObject = mantaClient.get(objectName);
        FileUtils.copyInputStreamToFile(mantaObject.getDataInputStream(), toFile);

        synchronized (fileTransfer) {
            fileTransfer.setPercentComplete(100);
            fileTransfer.setBytesToTransfer(0);
            Long contentLength = mantaObject.getContentLength();
            fileTransfer.setBytesTransferred(contentLength != null ? contentLength : -1L);
            fileTransfer.completeWithResult(toFile);
        }
    }

    @Nonnull
    @Override
    public String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }
}
