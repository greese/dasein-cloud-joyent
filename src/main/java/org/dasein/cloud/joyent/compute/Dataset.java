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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.compute.Architecture;
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
    public void downloadImage(@Nonnull String machineImageId, @Nonnull OutputStream toOutput) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No downloading images");
    }

    @Override
    public @Nullable MachineImage getMachineImage(@Nonnull String machineImageId) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context has been defined for this request");
        }
        JoyentMethod method = new JoyentMethod(provider);

        try {
            String json = method.doGetJson(provider.getEndpoint(), "datasets/" + machineImageId);
            
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
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale) {
        return "dataset";
    }

    @Override
    public boolean hasPublicLibrary() {
        return true;
    }

    @Override
    public @Nonnull AsynchronousTask<String> imageVirtualMachine(@Nonnull String vmId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No custom imaging");
    }

    @Override
    public @Nonnull AsynchronousTask<String> imageVirtualMachineToStorage(@Nonnull String vmId, @Nonnull String name, @Nonnull String description, @Nonnull String directory) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No custom imaging");
    }

    @Override
    public @Nonnull String installImageFromUpload(@Nonnull MachineImageFormat format, @Nonnull InputStream imageStream) throws CloudException, InternalException {
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
    public @Nonnull Iterable<MachineImage> listMachineImages() throws CloudException, InternalException {
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
                
                if( image != null ) {
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
    public @Nonnull Iterable<MachineImage> listMachineImagesOwnedBy(@Nullable String accountId) throws CloudException, InternalException {
        if( accountId == null || accountId.equals("") || accountId.equals("--joyent--") ) {
            return listMachineImages();
        }
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageFormat.AWS);
    }

    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String forMachineImageId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public @Nonnull String registerMachineImage(@Nonnull String atStorageLocation) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Registration not supported");
    }

    @Override
    public void remove(@Nonnull String machineImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Removal not supported");
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws CloudException, InternalException {
        ArrayList<MachineImage> images = new ArrayList<MachineImage>();
        
        for( MachineImage img : listMachineImages() ) {
            if( architecture != null ) {
                if( !architecture.equals(img.getArchitecture()) ) {
                    continue;
                }
            }
            if( platform != null && !platform.equals(Platform.UNKNOWN) ) {
                Platform p = img.getPlatform();
                
                if( p.equals(Platform.UNKNOWN) ) {
                    continue;
                }
                else if( platform.isWindows() ) {
                    if( !p.isWindows() ) {
                        continue;
                    }
                }
                else if( platform.equals(Platform.UNIX) ) {
                    if( !p.isUnix() ) {
                        continue;
                    }
                }
                else if( !platform.equals(p) ) {
                    continue;
                }
            }
            if( keyword != null ) {
                if( !img.getName().contains(keyword) ) {
                    if( !img.getDescription().contains(keyword) ) {
                        if( !img.getProviderMachineImageId().contains(keyword) ) {
                            continue;
                        }
                    }
                }
            }
            images.add(img);
        }
        return images;
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
    public boolean supportsImageSharing() {
        return false;
    }

    @Override
    public boolean supportsImageSharingWithPublic() {
        return false;
    }

    @Override
    public @Nonnull String transfer(@Nonnull CloudProvider fromCloud, @Nonnull String machineImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Cannot transfer datasets");
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
        image.setType(MachineImageType.STORAGE);
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
