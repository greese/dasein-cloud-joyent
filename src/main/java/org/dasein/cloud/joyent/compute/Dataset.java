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

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.AbstractImageSupport;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageFilterOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
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

public class Dataset extends AbstractImageSupport {
    private SmartDataCenter provider;
    
    Dataset(@Nonnull SmartDataCenter sdc) {
        super(sdc);
        provider = sdc;
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
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return "dataset";
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
    public @Nonnull Iterable<MachineImage> listImages(@Nullable ImageFilterOptions options) throws CloudException, InternalException {
        ImageClass cls = (options == null ? null : options.getImageClass());

        if( cls != null && !cls.equals(ImageClass.MACHINE) ) {
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

                if( image != null && (options == null || options.matches(image)) ) {
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

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Removal not supported");
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchImages(@Nullable String accountNumber, @Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass... imageClasses) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchPublicImages(@Nonnull ImageFilterOptions options) throws CloudException, InternalException {
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

                if( image != null && options.matches(image) ) {
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
