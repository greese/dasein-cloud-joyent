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

package org.dasein.cloud.joyent.compute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.compute.*;
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
    private DatasetCapabilities capabilities;

    Dataset(@Nonnull SmartDataCenter sdc) {
        super(sdc);
        provider = sdc;
    }

    @Override
    public ImageCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new DatasetCapabilities(provider);
        }
        return capabilities;
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
            return toMachineImage(new JSONObject(json));
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
        return getCapabilities().identifyLocalBundlingRequirement();
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
    public @Nonnull Iterable<MachineImage> listImages(@Nullable ImageFilterOptions options) throws CloudException, InternalException {
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

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Removal not supported");
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
                MachineImage image = toMachineImage(arr.getJSONObject(i));

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
        return ImageClass.MACHINE.equals(cls);
    }

    private @Nullable MachineImage toMachineImage(@Nullable JSONObject json) throws CloudException, InternalException {
        if( json == null ) {
            return null;
        }
        String regionId = getContext().getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region ID was specified for this request");
        }
        String imageId = null, name = null, description = null;
        Architecture architecture = Architecture.I64;
        Platform platform = Platform.UNKNOWN;
        long created = 0L;

        try {
            if( json.has("id") ) {
                imageId = json.getString("id");
            }
            if( json.has("name") ) {
                name = json.getString("name");
            }
            if( json.has("description") ) {
                description = json.getString("description");
            }
            if( json.has("os") ) {
                String os = (name == null ? json.getString("os") : name + " " + json.getString("os"));

                platform = Platform.guess(os);
                if( os.contains("32") ) {
                    architecture = Architecture.I32;
                }
            }
            if( json.has("created") ) {
                created = provider.parseTimestamp(json.getString("created"));
            }
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
        if( imageId == null ) {
            return null;
        }
        if( name == null ) {
            name = imageId;
        }
        if( description == null ) {
            description = name + " (" + platform + ") [#" + imageId + "]";
        }
        return MachineImage.getMachineImageInstance("--joyent--", regionId, imageId, MachineImageState.ACTIVE, name, description, architecture, platform).createdAt(created);
    }
}
