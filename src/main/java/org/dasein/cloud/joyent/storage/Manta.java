/**
 * Copyright (C) 2009-2013 Dell, Inc
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

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.exception.MantaClientHttpResponseException;
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
import org.dasein.cloud.util.CacheLevel;
import org.dasein.cloud.util.NamingConstraints;
import org.dasein.util.uom.storage.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.*;

/**
 * @author ilya.drabenia
 * @author anton.karavaev
 */

/**
 * Manta Object Store through Dasein vocabulary:
 * -- Bucket - directory
 * -- Object - file in the directory
 * -- Buckets can be nested - subdirectories
 * -- Root Objects are supported - root directory can contain files
 */
public class Manta implements BlobStoreSupport {
    public static final  String CUSTOM_PROP_STORAGE_URL = "storageUrl";
    private static final Logger logger                  = SmartDataCenter.getLogger(MantaStorageServices.class, "std");

    private final CloudProvider provider;
    private final MantaClient   mantaClient;
    private final String        regionId;
    private final String        rootPath;
    private final String        publicPath;

    public Manta( CloudProvider provider ) throws IOException, CloudException {
        this.provider = provider;
        this.regionId = provider.getContext().getRegionId();
        this.mantaClient = getClient();
        // TODO: make configurable
        this.rootPath = "/" + provider.getContext().getAccountNumber() + "/stor";
        this.publicPath = "/" + provider.getContext().getAccountNumber() + "/public";
    }

    private MantaClient getClient() throws CloudException, IOException {
        ProviderContext context = provider.getContext();

        List<ContextRequirements.Field> fields = provider.getContextRequirements().getConfigurableValues();
        String keyName = "";
        String privateKey = "";
        char[] keyPassword = null;
        for( ContextRequirements.Field f : fields ) {
            if( f.type.equals(ContextRequirements.FieldType.KEYPAIR) ) {
                byte[][] keyPair = ( byte[][] ) provider.getContext().getConfigurationValue(f);
                keyName = new String(keyPair[0], "utf-8");
                privateKey = new String(keyPair[1], "utf-8");
            }
            else if( f.type.equals(ContextRequirements.FieldType.PASSWORD) ) {
                byte[] password = ( byte[] ) provider.getContext().getConfigurationValue(f);
                if( password != null ) {
                    keyPassword = new String(password, "utf-8").toCharArray();
                }
            }
        }

        return MantaClient.newInstance(context.getCustomProperties().getProperty(CUSTOM_PROP_STORAGE_URL), context.getAccountNumber(), privateKey, keyName, keyPassword);
    }

    /**
     * Manta supports directories with sub-directories in /:login/stor or /:login/public.
     *
     *
     * @throws CloudException
     * @throws InternalException
     * @return
     */
    @Override
    public boolean allowsNestedBuckets() throws CloudException, InternalException {
        return true;
    }

    /**
     * Manta does not support objects on root level. However, user must specify one of two available storage folders:
     * /:login/stor or /:login/public which will be used as a root level.
     *
     * @throws CloudException
     * @throws InternalException
     * @return
     */
    @Override
    public boolean allowsRootObjects() throws CloudException, InternalException {
        return true;
    }

    /**
     * Manta allow public sharing using directory /:login/public
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
     * Manta deletes directory with content.
     *
     * @param bucket directory path
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public void clearBucket(@Nonnull String bucket) throws CloudException, InternalException {
        String path = toStoragePath(bucket, null, !isPublic(bucket, null));
        boolean retryRecursively = false;
        try {
            mantaClient.delete(path);
        } catch (MantaCryptoException e) {
            throw new CloudException(e);
        } catch (IOException e) {
            retryRecursively = true;
            logger.debug("Directory is not empty. Delete recursively.", e);
        } catch( MantaClientHttpResponseException e ) {
            retryRecursively = true;
            logger.debug("Directory is not empty. Delete recursively.", e);
        }
        if( retryRecursively ) {
            // if bucket is not empty remove recursively
            try {
                mantaClient.deleteRecursive(path);
            } catch( MantaCryptoException e ) {
                throw new CloudException(e);
            } catch( IOException e ) {
                throw new CloudException(e);
            } catch( MantaClientHttpResponseException e ) {
                throw new CloudException(e);
            }
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
            mantaClient.putDirectory(toStoragePath(bucket, null, true), null);
        } catch (IOException e) {
            throw new CloudException(e);
        } catch (MantaCryptoException e) {
            throw new CloudException(e);
        } catch( MantaClientHttpResponseException e ) {
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
        return getMantaObjectMetadata(bucket, null, true) != null
                || getMantaObjectMetadata(bucket, null, false) != null;
    }

//    private boolean checkMantaPathExists( @Nonnull String path ) throws InternalException, CloudException {
//        try {
//            mantaClient.head(path);
//            return true;
//        } catch (MantaCryptoException e) {
//            throw new CloudException(e);
//        } catch (MantaClientHttpResponseException e) {
//            if (e.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
//                if (e.getStatusCode() != HttpStatus.SC_FORBIDDEN) {
//                    throw new CloudException(e);
//                }
//            }
//        } catch (IOException e) {
//            throw new CloudException(e);
//        }
//        return false;
//    }

    /**
     * Returns {@link Blob} representation of Manta directory. Null if a bucket name is not a directory or a bucket not found
     *
     * @param bucketName directory path
     *
     * @return {@link Blob} representation of Manta directory
     *
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public @Nullable Blob getBucket(@Nonnull String bucketName) throws InternalException, CloudException {
        Blob bucket = null;
        // check private bucket first
        MantaObject mantaObject = getMantaObjectMetadata(bucketName, null);
        if( mantaObject == null ) { // this is 404
            return null;
        }
        if (isDirectory(mantaObject)) {
            bucket = Blob.getInstance(regionId, "", bucketName, new Date().getTime());
        }
        return bucket;
    }

    /**
     * {@link com.joyent.manta.client.MantaObject#isDirectory()} works only after listObjects(String path) method.
     *
     * @param mantaObject object with content type header
     * @return
     */
    private boolean isDirectory(@Nonnull MantaObject mantaObject) {
        return mantaObject.getHttpHeaders().getContentType().equals(MantaObject.DIRECTORY_HEADER);
    }

    /**
     * {@link com.joyent.manta.client.MantaObject#getContentLength()} works only after listObjects(String path) method.
     * Returns Double for convenient usage with {@link Storage}.
     *
     * @param mantaObject object with content
     * @return
     */
    private Double getContentLength(@Nonnull MantaObject mantaObject) {
        return mantaObject.getHttpHeaders().getContentLength().doubleValue();
    }

    /**
     * Returns {@link Blob} representation of {@link MantaObject}.
     *
     * @param bucketName directory path
     * @param objectName object name
     *
     * @return {@link Blob} representation of {@link MantaObject}.
     *
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public @Nullable Blob getObject(@Nullable String bucketName, @Nonnull String objectName)
            throws InternalException, CloudException {
        checkBucket(bucketName);
        MantaObject mantaObject = getMantaObjectMetadata(bucketName, objectName);
        if(mantaObject == null) { // this is 404
            return null;
        }
        return Blob.getInstance(regionId, "", bucketName, objectName, new Date().getTime(),
                new Storage<org.dasein.util.uom.storage.Byte>(getContentLength(mantaObject), Storage.BYTE));
    }

    /**
     * Returns {@link Storage} of {@link MantaObject}.
     *
     * @param bucketName directory path
     * @param objectName object name
     * @return {@link Storage} of {@link MantaObject}
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public @Nullable Storage<org.dasein.util.uom.storage.Byte> getObjectSize(@Nullable String bucketName,
                                                                             @Nullable String objectName)
            throws InternalException, CloudException {
        checkBucket(bucketName);
        Storage<org.dasein.util.uom.storage.Byte> storage = null;
        if (objectName != null) {
            MantaObject mantaObject = getMantaObjectMetadata(bucketName, objectName);
            if(mantaObject != null) {
                storage = new Storage<org.dasein.util.uom.storage.Byte>(getContentLength(mantaObject), Storage.BYTE);
            }
        }
        return storage;
    }

    /**
     * Loads {@link MantaObject} without it`s content.
     *
     * @param bucket bucket name
     * @param object object name
     * @return Manta object
     */
    private @Nullable MantaObject getMantaObjectMetadata( @Nullable String bucket, @Nullable String object, boolean isPrivate )
            throws CloudException, InternalException {
        try {
            return mantaClient.head(toStoragePath(bucket, object, isPrivate));
        } catch (MantaClientHttpResponseException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return null;
            }
            throw new CloudException(e);
        } catch (IOException e) {
            throw new CloudException(e);
        } catch (MantaCryptoException e) {
            throw new InternalException(e);
        }
    }

    private @Nullable MantaObject getMantaObjectMetadata( @Nullable String bucket, @Nullable String object) throws CloudException, InternalException {
        MantaObject o = getMantaObjectMetadata(bucket, object, true);
        if( o == null ) {
            o = getMantaObjectMetadata(bucket, object, false);
        }
        return o;
    }

    /**
     * According to this <a href=http://apidocs.joyent.com/manta/#directories>doc</a> there is no limit for directories
     * and sub-directories.
     *
     * @return max directories
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public int getMaxBuckets() throws CloudException, InternalException {
        return Integer.MAX_VALUE;
    }

    /**
     * According to this <a href=http://apidocs.joyent.com/manta/#directories>doc</a> there is no limit for object size.
     * @return max object size
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public Storage<org.dasein.util.uom.storage.Byte> getMaxObjectSize() throws InternalException, CloudException {
        return new Storage<org.dasein.util.uom.storage.Byte>(Long.MAX_VALUE, Storage.BYTE);
    }

    /**
     * According to this <a href=http://apidocs.joyent.com/manta/#directories>doc</a> Manta limits objects per single
     * directory to 1,000,000.
     *
     * @return objects limit per single directory
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public int getMaxObjectsPerBucket() throws CloudException, InternalException {
        return 1000000;
    }

    private final char[] BUCKET_CHARS = {'-', '.'};

    @Override
    public @Nonnull
    NamingConstraints getBucketNameRules() throws CloudException, InternalException {
        return NamingConstraints.getAlphaNumeric(1, 255).lowerCaseOnly().limitedToLatin1().constrainedBy(BUCKET_CHARS);
    }

    private final char[] OBJECT_CHARS = {'-', '.', ',', '#', '+'};
    @Override
    public @Nonnull NamingConstraints getObjectNameRules() throws CloudException, InternalException {
        return NamingConstraints.getAlphaNumeric(1, 255).lowerCaseOnly().limitedToLatin1().constrainedBy(OBJECT_CHARS);
    }

    /**
     * Provider term for bucket in Manta is "directory".
     *
     * @param locale
     * @return
     */
    @Nonnull
    @Override
    public String getProviderTermForBucket(@Nonnull Locale locale) {
        return "directory";
    }

    /**
     *
     * @param locale
     * @return
     */
    @Nonnull
    @Override
    public String getProviderTermForObject(@Nonnull Locale locale) {
        return "object";
    }

    /**
     * Manta public storage is located in /:login/public/ directory.
     *
     * @param bucket directory path
     * @param object object name is not used since manta checks only directory path
     * @return is the storage public or not
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public boolean isPublic(@Nullable String bucket, @Nullable String object) throws CloudException, InternalException {
        return getMantaObjectMetadata(bucket, object, false) != null;
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

        org.dasein.cloud.util.Cache<Boolean> cache = org.dasein.cloud.util.Cache.getInstance(provider,
                "Blob.isSubscribed", Boolean.class, CacheLevel.REGION_ACCOUNT);
        final ProviderContext context = provider.getContext();
        final Iterable<Boolean> cachedIsSubscribed = cache.get(context);
        if (cachedIsSubscribed != null && cachedIsSubscribed.iterator().hasNext()) {
            final Boolean isSubscribed = cachedIsSubscribed.iterator().next();
            if (isSubscribed != null) {
                return isSubscribed;
            }
        }

        try {
            mantaClient.listObjects(rootPath);
            cache.put(context, Collections.singleton(true));
            return true;
        } catch (MantaClientHttpResponseException ex) {
            if (ex.getStatusCode() == HttpStatus.SC_FORBIDDEN) {
                cache.put(context, Collections.singleton(false));
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
        checkBucket(bucket);
        Collection<MantaObject> mantaObjects;
        Collection<Blob> result = new ArrayList<Blob>();
        try {
            mantaObjects = mantaClient.listObjects(toStoragePath(bucket, null, !isPublic(bucket, null)));
        } catch (MantaCryptoException e) {
            throw new CloudException(e);
        } catch (IOException e) {
            throw new CloudException(e);
        } catch (MantaObjectException e) {
            throw new CloudException(e);
        } catch( MantaClientHttpResponseException e ) {
            throw new CloudException(e);
        }
        for (MantaObject mantaObject : mantaObjects) {
            String dirName = parsePath(mantaObject.getPath());
            if (mantaObject.isDirectory()) {
                result.add(Blob.getInstance(regionId, "", bucket, new Date().getTime()));
            } else {
                String objectName = parseObjectName(mantaObject.getPath());
                result.add(Blob.getInstance(regionId, "", bucket, objectName, new Date().getTime(),
                        new Storage<org.dasein.util.uom.storage.Byte>(mantaObject.getContentLength(), Storage.BYTE)
                ));
            }
        }
        return result;
    }

    /**
     * Manta has to move directory to /:login/public to make directory public. It violates Dasein rules.
     * Method throws {@link OperationNotSupportedException}.
     *
     * @param bucket
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public void makePublic(@Nonnull String bucket) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Not supported yet");
    }

    /**
     * Manta has to move directory to /:login/public to make directory public. It violates Dasein rules.
     * Method throws {@link OperationNotSupportedException}.
     *
     * @param bucket Manta does not support buckets. This parameter is ignored.
     * @param object
     * @throws InternalException
     * @throws CloudException
     */
    @Override
    public void makePublic(@Nullable String bucket, @Nonnull String object) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Not supported yet");
    }

    /**
     * Manta does not support buckets. Method throws {@link OperationNotSupportedException}.
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
        throw new OperationNotSupportedException("Manta does not have support of buckets");
    }

    /**
     * Deletes directory with contents.
     *
     * @param bucket path
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public void removeBucket(@Nonnull String bucket) throws CloudException, InternalException {
        boolean retryRecursively = false;
        String path = toStoragePath(bucket, null, !isPublic(bucket, null));
        try {
            mantaClient.delete(path);
        } catch (MantaCryptoException e) {
            throw new CloudException(e);
        } catch (IOException e) {
            retryRecursively = true;
            logger.debug("Directory is not empty. Delete recursively.", e);
        } catch( MantaClientHttpResponseException e ) {
            retryRecursively = true;
            logger.debug("Directory is not empty. Delete recursively.", e);
        }
        if( retryRecursively ) {
            // if bucket is not empty remove recursively
            try {
                mantaClient.deleteRecursive(path);
            } catch (MantaCryptoException ex) {
                throw new CloudException(ex);
            } catch (MantaClientHttpResponseException ex) {
                throw new CloudException(ex);
            } catch (IOException ex) {
                throw new CloudException(ex);
            }
        }
    }

    /**
     * Method remove file.
     *
     * @param bucket Path to directory. Null is not supported
     * @param object Manta object name
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public void removeObject(@Nullable String bucket, @Nonnull String object) throws CloudException, InternalException {
        checkBucket(bucket);
        try {
            mantaClient.delete(toStoragePath(bucket, object, !isPublic(bucket, object)));
        } catch (MantaCryptoException e) {
            throw new CloudException(e);
        } catch (IOException e) {
            throw new CloudException(e);
        } catch( MantaClientHttpResponseException e ) {
            throw new CloudException(e);
        }
    }

    /**
     * Manta does not support directory linking. Method throws {@link OperationNotSupportedException}.
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
        throw new OperationNotSupportedException("Not supported yet");
    }

    /**
     * Method rename object. It creates hard link and remove original link to file.
     *
     * @param bucket directory path
     * @param oldName old object name
     * @param newName new object name
     * @throws CloudException
     * @throws InternalException
     */
    @Override
    public void renameObject(@Nullable String bucket, @Nonnull String oldName, @Nonnull String newName) throws
            CloudException, InternalException {
        String path = toStoragePath(bucket, oldName, !isPublic(bucket, oldName));
        String linkPath = path + parseObjectName(newName);
        String objPath = path + parseObjectName(oldName);
        try {
            mantaClient.putSnapLink(linkPath, objPath, null);
            mantaClient.delete(objPath);
        } catch (MantaCryptoException e) {
            throw new CloudException(e);
        } catch (IOException e) {
            throw new CloudException(e);
        } catch( MantaClientHttpResponseException e ) {
            throw new CloudException(e);
        }
    }

    /**
     * Method uploads {@code sourceFile} to Manta {@code bucket} with {@code objectName}.
     *
     * @param sourceFile file that will be uploaded
     * @param bucket path to Manta object. Null means root and not supported by Manta {@link Manta#allowsRootObjects}.
     * @param objectName Manta object name
     * @return representation of uploaded file
     * @throws CloudException
     * @throws InternalException
     */
    @Nonnull
    @Override
    public Blob upload(@Nonnull File sourceFile, @Nullable String bucket, @Nonnull String objectName) throws
            CloudException, InternalException {
        if( bucket == null ) {
            bucket = "";
        }
        checkBucket(bucket);
        String pathToDir = toStoragePath(bucket, null, true);
        String validObjectName = parseObjectName(objectName);
        try {
            if (!exists(pathToDir)) {
                createBucket(bucket, false);
            }

            // todo: may be moved to put method from AbstractBlobStoreSupport
            MantaObject mantaObject = new MantaObject(pathToDir + "/" + validObjectName);
            mantaObject.setDataInputStream(new FileInputStream(sourceFile));
            mantaClient.put(mantaObject);

            return Blob.getInstance(regionId, "", bucket, validObjectName , new Date().getTime(),
                    new Storage<org.dasein.util.uom.storage.Byte>(sourceFile.length(), Storage.BYTE));
        } catch (IOException e) {
            throw new CloudException(e);
        } catch (MantaCryptoException e) {
            throw new CloudException(e);
        } catch( MantaClientHttpResponseException e ) {
            throw new CloudException(e);
        }
    }

    //TODO: remove, root objects ARE supported by Manta
    private void checkBucket(@Nullable String bucket) throws OperationNotSupportedException {
//        if (bucket == null || bucket.trim().isEmpty()) {
//            throw new OperationNotSupportedException("Root objects are not supported");
//        }
    }

//    /**
//     * Makes path a Manta private storage directory path.
//     *
//     * @param path directory path
//     * @return Manta directory path
//     */
//    private @Nonnull String coerceToDirectory(@Nonnull String path) {
//        if( path == null ) {
//            path = "";
//        }
//        String pathToDir = path.trim();
//        if (!pathToDir.startsWith(rootPath)) {
//            pathToDir = rootPath + "/" + pathToDir;
//        }
//        if (!pathToDir.endsWith("/")) {
//            pathToDir += "/";
//        }
//        return pathToDir;
//    }

    private @Nonnull String toPath(@Nullable String bucket, @Nullable String object) {
        String path = "/";
        if( bucket != null ) {
            path += bucket;
        }
        if( object != null ) {
            if( !path.endsWith("/") && !object.startsWith("/")) {
                path += "/";
            }
            path += object;
        }
        return path;
    }

    private @Nonnull String toStoragePath(@Nullable String bucket, @Nullable String object, boolean isPrivate) {
        if( isPrivate ) {
            return rootPath + toPath(bucket, object);
        }
        else {
            return publicPath + toPath(bucket, object);
        }
    }

    /**
     * Returns path without object name.
     *
     * @param objectName full path
     * @return directory path
     */
    private @Nonnull String parsePath(@Nonnull String objectName) {
        return objectName.substring(0, objectName.lastIndexOf('/') + 1);
    }

    /**
     * Returns object name without path.
     *
     * @param path full path
     * @return object name
     */
    private @Nonnull String parseObjectName(@Nonnull String path) {
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
    public FileTransfer download(final @Nullable String bucket, @Nonnull final String objectName, final @Nonnull File toFile)
            throws InternalException, CloudException {
        checkBucket(bucket);
        final FileTransfer fileTransfer = new FileTransfer();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    processDownloadAsync(fileTransfer, toStoragePath(bucket, objectName, !isPublic(bucket, objectName)), toFile);
                } catch (Exception ex) {
                    logger.error("Error on file download from Manta Storage", ex);
                    fileTransfer.complete(ex);
                }
            }
        }).start();

        return fileTransfer;
    }

    private void processDownloadAsync(FileTransfer fileTransfer, String path, File toFile) throws IOException, MantaCryptoException, MantaClientHttpResponseException {

        // need to synchronize because variables in task is not synchronized properly
        synchronized (fileTransfer) {
            fileTransfer.setStartTime(new Date().getTime());
            fileTransfer.setPercentComplete(0);
        }

        MantaObject mantaObject = mantaClient.get(path);
        FileUtils.copyInputStreamToFile(mantaObject.getDataInputStream(), toFile);

        synchronized (fileTransfer) {
            fileTransfer.setPercentComplete(100);
            fileTransfer.setBytesToTransfer(0);
            fileTransfer.setBytesTransferred(getContentLength(mantaObject).longValue());
            fileTransfer.completeWithResult(toFile);
        }

    }

    @Override
    public String getSignedObjectUrl(@Nonnull String bucket, @Nonnull String object, @Nonnull String expiresEpochInSeconds) throws InternalException, CloudException{
        throw new OperationNotSupportedException("Signed object URLs are not currently supported.");
    }

    @Nonnull
    @Override
    public String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }
}
