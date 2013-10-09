package org.dasein.cloud.joyent.storage;

import com.google.api.client.http.HttpResponseException;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.exception.MantaCryptoException;
import com.joyent.manta.exception.MantaObjectException;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Locale;

/**
 * @author ilya.drabenia
 */
public class Manta implements BlobStoreSupport {
    private static final Logger logger = SmartDataCenter.getLogger(MantaStorageServices.class, "std");

    private final CloudProvider provider;
    private final MantaClient mantaClient;
    private final String regionId;

    public Manta(CloudProvider provider) throws IOException, CloudException {
        this.provider = provider;
        this.regionId = provider.getContext().getRegionId();
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
     * Manta supports directories with sub-directories in /:login/stor or /:login/public but Manta Java API is not, yet.
     *
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
     *
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
     *
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public boolean allowsPublicSharing() throws CloudException, InternalException {
        return true;
    }

    /**
     * Manta deletes all directory content and creates an empty directory with the same name.
     *
     * @param bucket directory path
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public void clearBucket(@Nonnull String bucket) throws CloudException, InternalException {
        String directoryName = parseDirectoryName(bucket);
        try {
            mantaClient.deleteRecursive(directoryName);
            mantaClient.putDirectory(directoryName, null);
        } catch (MantaCryptoException e) {
            throw new CloudException(e);
        } catch (IOException e) {
            throw new CloudException(e);
        }
    }

    /**
     * Manta creates new directory.
     *
     * @param bucket directory path
     * @param findFreeName is not supported and ignored
     * @return cloud storage object
     * @throws InternalException
     * @throws CloudException
     */
    @Nonnull
    @Override
    public Blob createBucket(@Nonnull String bucket, boolean findFreeName) throws InternalException, CloudException {
        try {
            mantaClient.putDirectory(bucket, null);
        } catch (IOException e) {
            throw new CloudException(e);
        } catch (MantaCryptoException e) {
            throw new CloudException(e);
        }
        return Blob.getInstance(regionId, "", bucket, new Date().getTime());
    }

    /**
     * Checks if bucket exists. Gets directory metadata, if anything returned, bucket exists.
     *
     * @param bucket directory path
     * @return true if bucket exists, false otherwise
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public boolean exists(@Nonnull String bucket) throws InternalException, CloudException {
        MantaObject mantaObject;
        try {
            mantaObject = mantaClient.head(bucket);
        } catch (MantaCryptoException e) {
            throw new CloudException(e);
        } catch (IOException e) {
            throw new CloudException(e);
        }
        return mantaObject != null;
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     *
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
    public Blob getObject(@Nullable String bucketName, @Nonnull String objectName) throws InternalException,
            CloudException {
//        MantaObject mantaObject = mantaClient.head(objectName);


        return null;
    }

    /**
     * Manta does not support this operation.
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
        throw new UnsupportedOperationException("Manta does not support operation");
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     *
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
        return new Storage<Byte>(Long.MAX_VALUE, Storage.BYTE);
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     *
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
     *
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
        // list of allowed characters may be incomplete
        return NameRules.getInstance(1, Integer.MAX_VALUE, true, true, false, new char[] {'-', '.', '\\'});
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     *
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
        // TODO: Unknown semantics
        return "object";
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
        if (object != null) {
            String accountName = this.provider.getContext().getAccountNumber();
            return object.startsWith("/" + accountName + "/public");
        } else {
            return false;
        }
    }

    /**
     * Method check if access to cloud is available
     *
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        try {
            mantaClient.listObjects("/" + provider.getContext().getAccountNumber() + "/stor");

            return true;
        } catch (HttpResponseException ex) {
            if (ex.getStatusCode() == HttpStatus.SC_FORBIDDEN) {
                return false;
            }

            throw new CloudException(ex);
        } catch (Exception ex) {
            throw new CloudException(ex);
        }
    }

    @Nonnull
    @Override
    public Iterable<Blob> list(@Nullable String bucket) throws CloudException, InternalException {
        if (bucket == null) {
            throw new OperationNotSupportedException("Bucket is a directory in Manta and it cannot be null");
        }
        Collection<MantaObject> mantaObjects;
        Collection<Blob> result = new ArrayList<Blob>();
        try {
            mantaObjects = mantaClient.listObjects(bucket);
        } catch (MantaCryptoException e) {
            throw new CloudException(e);
        } catch (IOException e) {
            throw new CloudException(e);
        } catch (MantaObjectException e) {
            throw new CloudException(e);
        }
        for (MantaObject mantaObject : mantaObjects) {
            String dirName = parseDirectoryName(mantaObject.getPath());
            if (mantaObject.isDirectory()) {
                result.add(Blob.getInstance(regionId, "", dirName, new Date().getTime()));
            } else {
                String objectName = parseObjectName(mantaObject.getPath());
                result.add(Blob.getInstance(regionId, "", dirName, objectName, new Date().getTime(),
                        new Storage<Byte>(mantaObject.getContentLength(), Storage.BYTE)
                ));
            }
        }
        return result;
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     *
     * @param bucket
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public void makePublic(@Nonnull String bucket) throws InternalException, CloudException {
        throw new UnsupportedOperationException("Manta does not have support of buckets");
    }

    /**
     * For make object public we need to change folder of object.
     *
     * @param bucket Manta does not support buckets. This parameter is ignored.
     * @param object
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public void makePublic(@Nullable String bucket, @Nonnull String object) throws InternalException, CloudException {
        throw new UnsupportedOperationException("");
    }

    /**
     * Manta does not support buckets. Method throws {@link UnsupportedOperationException}.
     *
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
     *
     * @param bucket Manta does not support buckets. This parameter is ignored.
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public void removeBucket(@Nonnull String bucket) throws CloudException, InternalException {
        throw new UnsupportedOperationException();
    }

    /**
     * Method remove file.
     *
     * @param bucket Manta does not support buckets. This parameter is ignored.
     * @param object Path to file on Manta
     * @throws CloudException
     * @throws InternalException
     */
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
     *
     * @param oldName
     * @param newName
     * @param findFreeName
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Nonnull
    @Override
    public String renameBucket(@Nonnull String oldName, @Nonnull String newName, boolean findFreeName) throws
            CloudException, InternalException {
        throw new UnsupportedOperationException();
    }

    /**
     * Method rename object. It creates hard link and remove original link to file.
     *
     * @param bucket Manta does not support buckets. This parameter is ignored.
     * @param oldName
     * @param newName
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public void renameObject(@Nullable String bucket, @Nonnull String oldName, @Nonnull String newName) throws
            CloudException, InternalException {
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
     *
     * @param sourceFile
     * @param bucket Manta does not support buckets. This parameter is ignored.
     * @param objectName
     * @return
     * @throws CloudException
     * @throws InternalException
     */
    @Nonnull
    @Override
    public Blob upload(@Nonnull File sourceFile, @Nullable String bucket, @Nonnull String objectName) throws
            CloudException, InternalException {
        try {
            return processFileUpload(sourceFile, objectName);
        } catch (IOException ex) {
            throw new CloudException(ex);
        } catch (MantaCryptoException ex) {
            throw new CloudException(ex);
        }
    }

    /**
     * Method create directory and upload file to it
     *
     * @param sourceFile file that will be uploaded
     * @param objectName path to file in Manta Storage
     * @return representation of uploaded file
     * @throws IOException
     * @throws MantaCryptoException
     */
    private Blob processFileUpload(@Nonnull File sourceFile, @Nonnull String objectName) throws IOException,
            MantaCryptoException {
        String dirName = parseDirectoryName(objectName);
        mantaClient.putDirectory(dirName, null);

        MantaObject mantaObject = new MantaObject(objectName);
        mantaObject.setDataInputStream(new FileInputStream(sourceFile));
        mantaClient.put(mantaObject);

        return Blob.getInstance(regionId, "", dirName, objectName , new Date().getTime(),
                new Storage<Byte>(sourceFile.length(), Storage.BYTE));
    }

    private String parseDirectoryName(@Nonnull String objectName) {
        return objectName.substring(0, objectName.lastIndexOf('/') + 1);
    }

    private String parseObjectName(@Nonnull String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    /**
     * Method download file {@code objectName} from Manta to file {@code toFile}. Action occurs asynchronous.
     * @param bucket Manta does not support buckets. This parameter is ignored.
     * @param objectName
     * @param toFile
     *
     * @return
     *
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

        // need to synchronize because variables in task is not synchronized properly
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
