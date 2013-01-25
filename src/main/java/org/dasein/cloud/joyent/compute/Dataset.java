/**
 * Copyright (C) 2009-2012 enStratus Networks Inc
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

package org.dasein.cloud.joyent.compute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageCreateOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageSupport;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.joyent.JoyentMethod;
import org.dasein.cloud.joyent.SmartDataCenter;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Dataset implements MachineImageSupport {
    private SmartDataCenter provider;
    
    Dataset(@Nonnull SmartDataCenter sdc) { provider = sdc; }

    @Override
    public void addImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image sharing is not supported");
    }

    @Override
    public void addPublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image sharing is not supported");
    }

    @Override
    public @Nonnull String bundleVirtualMachine(@Nonnull String virtualMachineId, @Nonnull MachineImageFormat format, @Nonnull String bucket, @Nonnull String name) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image bundling is not supported");
    }

    @Override
    public void bundleVirtualMachineAsync(@Nonnull String virtualMachineId, @Nonnull MachineImageFormat format, @Nonnull String bucket, @Nonnull String name, @Nonnull AsynchronousTask<String> trackingTask) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image bundling is not supported");
    }

    @Override
    public @Nonnull MachineImage captureImage(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Custom machine images are not supported");
    }

    @Override
    public void captureImageAsync(@Nonnull ImageCreateOptions options, @Nonnull AsynchronousTask<MachineImage> taskTracker) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Custom machine images are not supported");
    }

    @Override
    public MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context has been defined for this request");
        }
        JoyentMethod method = new JoyentMethod(provider);

        try {
            String json = method.doGetJson(provider.getEndpoint(), "datasets/" + providerImageId);

            if( json == null ) {
                return null;
            }
            return toMachineImage(ctx, new JSONObject(json));
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    @Override
    @Deprecated
    public @Nullable MachineImage getMachineImage(@Nonnull String machineImageId) throws CloudException, InternalException {
        return getImage(machineImageId);
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale) {
        return getProviderTermForImage(locale, ImageClass.MACHINE);
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return "dataset";
    }

    @Override
    public @Nonnull String getProviderTermForCustomImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return getProviderTermForImage(locale, cls);
    }

    @Override
    public boolean hasPublicLibrary() {
        return true;
    }

    @Override
    public @Nonnull Requirement identifyLocalBundlingRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull AsynchronousTask<String> imageVirtualMachine(@Nonnull String vmId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No custom imaging");
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        JoyentMethod method = new JoyentMethod(provider);

        method.doGetJson(provider.getEndpoint(), "datasets");
        return true;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listImageStatus(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nonnull ImageClass cls, @Nonnull String ownedBy) throws CloudException, InternalException {
        if( !cls.equals(ImageClass.MACHINE) ) {
            return Collections.emptyList();
        }
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context has been defined for this request");
        }
        JoyentMethod method = new JoyentMethod(provider);

        try {
            JSONArray arr = new JSONArray(method.doGetJson(provider.getEndpoint(), "datasets"));
            ArrayList<MachineImage> images = new ArrayList<MachineImage>();

            for( int i=0; i<arr.length(); i++ ) {
                MachineImage image = toMachineImage(ctx, arr.getJSONObject(i));

                if( image != null && ownedBy.equals(image.getProviderOwnerId()) ) {
                    images.add(image);
                }
            }
            return images;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    @Override
    public @Nonnull Iterable<MachineImage> listMachineImages() throws CloudException, InternalException {
        return listImages(ImageClass.MACHINE);
    }

    @Override
    public @Nonnull Iterable<MachineImage> listMachineImagesOwnedBy(@Nullable String accountId) throws CloudException, InternalException {
        if( accountId == null || accountId.equals("") || accountId.equals("--joyent--") ) {
            return listMachineImages();
        }
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormatsForBundling() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String forMachineImageId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<ImageClass> listSupportedImageClasses() throws CloudException, InternalException {
        return Collections.singletonList(ImageClass.MACHINE);
    }

    @Override
    public @Nonnull Iterable<MachineImageType> listSupportedImageTypes() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageType.VOLUME);
    }

    private boolean matches(@Nonnull MachineImage image, @Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) {
        if( architecture != null && !architecture.equals(image.getArchitecture()) ) {
            return false;
        }
        if( platform != null && !platform.equals(Platform.UNKNOWN) ) {
            Platform mine = image.getPlatform();

            if( platform.isWindows() && !mine.isWindows() ) {
                return false;
            }
            if( platform.isUnix() && !mine.isUnix() ) {
                return false;
            }
            if( platform.isBsd() && !mine.isBsd() ) {
                return false;
            }
            if( platform.isLinux() && !mine.isLinux() ) {
                return false;
            }
            if( platform.equals(Platform.UNIX) ) {
                if( !mine.isUnix() ) {
                    return false;
                }
            }
            else if( !platform.equals(mine) ) {
                return false;
            }
        }
        if( keyword != null ) {
            keyword = keyword.toLowerCase();
            if( !image.getDescription().toLowerCase().contains(keyword) ) {
                if( !image.getName().toLowerCase().contains(keyword) ) {
                    if( !image.getProviderMachineImageId().toLowerCase().contains(keyword) ) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public @Nonnull MachineImage registerImageBundle(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No support for image registration");
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void remove(@Nonnull String machineImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Removal not supported");
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Removal not supported");
    }

    @Override
    public void removeAllImageShares(@Nonnull String providerImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image sharing is not supported");
    }

    @Override
    public void removeImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image sharing is not supported");
    }

    @Override
    public void removePublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image sharing is not supported");
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws CloudException, InternalException {
        ArrayList<MachineImage> matches = new ArrayList<MachineImage>();

        for( MachineImage img : searchImages(null, keyword, platform, architecture, ImageClass.MACHINE) ) {
            matches.add(img);
        }
        for( MachineImage img : searchPublicImages(keyword, platform, architecture, ImageClass.MACHINE) ) {
            matches.add(img);
        }
        return matches;
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchImages(@Nullable String accountNumber, @Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass... imageClasses) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchPublicImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass... imageClasses) throws CloudException, InternalException {
        if( imageClasses != null && imageClasses.length > 0 ) {
            boolean machine = false;

            for( ImageClass cls : imageClasses ) {
                if( !cls.equals(ImageClass.MACHINE) ) {
                    machine = true;
                }
            }
            if( !machine ) {
                return Collections.emptyList();
            }
        }
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context has been defined for this request");
        }
        JoyentMethod method = new JoyentMethod(provider);

        try {
            JSONArray arr = new JSONArray(method.doGetJson(provider.getEndpoint(), "datasets"));
            ArrayList<MachineImage> images = new ArrayList<MachineImage>();

            for( int i=0; i<arr.length(); i++ ) {
                MachineImage image = toMachineImage(ctx, arr.getJSONObject(i));

                if( image != null && matches(image, keyword, platform, architecture) ) {
                    images.add(image);
                }
            }
            return images;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    @Override
    public void shareMachineImage(@Nonnull String machineImageId, @Nullable String withAccountId, boolean allow) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Sharing not supported");
    }

    @Override
    public boolean supportsCustomImages() {
        return false;
    }

    @Override
    public boolean supportsDirectImageUpload() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageCapture(@Nonnull MachineImageType type) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageSharing() {
        return false;
    }

    @Override
    public boolean supportsImageSharingWithPublic() {
        return false;
    }

    @Override
    public boolean supportsPublicLibrary(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return true;
    }

    @Override
    public void updateTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }

    @Override
    public void updateTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }

    @Override
    public void removeTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }

    @Override
    public void removeTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }
    private @Nullable MachineImage toMachineImage(@Nonnull ProviderContext ctx, @Nullable JSONObject json) throws JSONException {
        if( json == null ) {
            return null;
        }
        MachineImage image = new MachineImage();
        
        image.setCurrentState(MachineImageState.ACTIVE);
        image.setProviderOwnerId("--joyent--");
        image.setProviderRegionId(ctx.getRegionId());
        image.setSoftware("");
        image.setTags(new HashMap<String,String>());
        image.setType(MachineImageType.VOLUME);
        image.setImageClass(ImageClass.MACHINE);
        if( json.has("id") ) {
            image.setProviderMachineImageId(json.getString("id"));
        }
        if( json.has("name") ) {
            image.setName(json.getString("name"));
        }
        if( json.has("os") ) {
            String os = (image.getName() + " " + json.getString("os"));
            
            if( os.contains("64") ) {
                image.setArchitecture(Architecture.I64);
            }
            else if( os.contains("32") ) {
                image.setArchitecture(Architecture.I32);
            }
            else {
                image.setArchitecture(Architecture.I64);
            }
            image.setPlatform(Platform.guess(os));
        }
        if( json.has("created") ) {
            // TODO: implement creation timestamps in dasein cloud
        }
        if( image.getProviderMachineImageId() == null ) {
            return null;
        }
        if( image.getName() == null ) {
            image.setName(image.getProviderMachineImageId());
        }
        if( image.getDescription() == null ) {
            image.setDescription(image.getName());
        }
        return image;
    }
}
