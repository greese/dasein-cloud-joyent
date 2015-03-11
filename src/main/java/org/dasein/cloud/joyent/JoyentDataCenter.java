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

package org.dasein.cloud.joyent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class JoyentDataCenter implements DataCenterServices {
    private SmartDataCenter provider;
    
    public JoyentDataCenter(@Nonnull SmartDataCenter sdc) { provider = sdc; }

    private transient volatile JoyentDataCenterCapabilities capabilities;
    @Nonnull
    @Override
    public DataCenterCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new JoyentDataCenterCapabilities(provider);
        }
        return capabilities;
    }

    @Override
    public @Nullable DataCenter getDataCenter(@Nonnull String providerDataCenterId) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context exists for this request");
        }
        String regionId = ctx.getRegionId();
        
        if( regionId == null ) {
            throw new CloudException("No data center is established for this request");
        }
        for( DataCenter dc : listDataCenters(regionId) ) {
            if( dc.getProviderDataCenterId().equals(providerDataCenterId) ) {
                return dc;
            }
        }
        return null;
    }

    @Override
    public @Nonnull String getProviderTermForDataCenter(@Nonnull Locale locale) {
        return "dc zone";
    }

    @Override
    public @Nonnull String getProviderTermForRegion(@Nonnull Locale locale) {
        return "data center";
    }

    @Override
    public @Nullable Region getRegion(@Nonnull String providerRegionId) throws InternalException, CloudException {
        for( Region region : listRegions() ) {
            if( region.getProviderRegionId().equals(providerRegionId) ) {
                return region;
            }
        }
        return null;
    }

    @Override
    public @Nonnull Collection<DataCenter> listDataCenters(@Nonnull String providerRegionId) throws InternalException, CloudException {
        Region r = getRegion(providerRegionId);
        
        if( r == null ) {
            throw new CloudException("No such region: " + providerRegionId);
        }
        DataCenter dc = new DataCenter();
        dc.setActive(true);
        dc.setAvailable(true);
        dc.setName(r.getName() + "a");
        dc.setProviderDataCenterId(r.getProviderRegionId() + "a");
        dc.setRegionId(r.getProviderRegionId());
        return Collections.singletonList(dc);
    }

    @Override
    public @Nonnull Collection<Region> listRegions() throws InternalException, CloudException {
        JoyentMethod method = new JoyentMethod(provider);        
        String json = method.doGetJson(provider.getEndpoint(), "datacenters");

        try {
            ArrayList<Region> regions = new ArrayList<Region>();
            
            JSONObject ob = new JSONObject(json);
            JSONArray ids = ob.names();
            
            for( int i=0; i<ids.length(); i++ ) {
                String regionId = ids.getString(i);
                Region r = new Region();
                
                r.setActive(true);
                r.setAvailable(true);
                r.setJurisdiction("US");
                r.setName(regionId);
                r.setProviderRegionId(regionId);
                regions.add(r);
            }
            return regions;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    @Override
    public @Nonnull Collection<ResourcePool> listResourcePools(String providerDataCenterId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public ResourcePool getResourcePool(String providerResourcePoolId) throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull Collection<StoragePool> listStoragePools() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public StoragePool getStoragePool(String providerStoragePoolId) throws InternalException, CloudException {
        return null;
    }

    @Nonnull
    @Override
    public Collection<Folder> listVMFolders() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Folder getVMFolder(String providerVMFolderId) throws InternalException, CloudException {
        return null;
    }
}
