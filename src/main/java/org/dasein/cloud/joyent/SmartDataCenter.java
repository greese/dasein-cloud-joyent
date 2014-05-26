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

package org.dasein.cloud.joyent;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.joyent.compute.JoyentComputeServices;
import org.dasein.cloud.joyent.storage.MantaStorageServices;
import org.dasein.cloud.storage.StorageServices;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SmartDataCenter extends AbstractCloud {
    static private @Nonnull String getLastItem(@Nonnull String name) {
        int idx = name.lastIndexOf('.');
        
        if( idx < 0 ) {
            return name;
        }
        else if( idx == (name.length()-1) ) {
            return "";
        }
        return name.substring(idx+1);
    }
    
    static public @Nonnull Logger getLogger(@Nonnull Class<?> cls, @Nonnull String type) {
        String pkg = getLastItem(cls.getPackage().getName());
        
        if( pkg.equals("joyent") ) {
            pkg = "";
        }
        else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.joyent." + type + "." + pkg + getLastItem(cls.getName()));
    }
    
    public SmartDataCenter() { }

    @Override
    public @Nonnull synchronized StorageServices getStorageServices() {
        return new MantaStorageServices(this);
    }

    @Override
    public @Nonnull JoyentComputeServices getComputeServices() {
        return new JoyentComputeServices(this);
    }

    public static final String DSN_SSH_KEY          = "sshKey";
    public static final String DSN_SSH_KEY_PASSWORD = "sshKeyPassword";

    @Override
    public @Nonnull ContextRequirements getContextRequirements() {
        return new ContextRequirements(
                new ContextRequirements.Field(DSN_SSH_KEY, "Private SSH Key stored in Joyent", ContextRequirements.FieldType.KEYPAIR, ContextRequirements.Field.ACCESS_KEYS, true),
                new ContextRequirements.Field(DSN_SSH_KEY_PASSWORD, "Password of ssh key uploaded to Joyent", ContextRequirements.FieldType.PASSWORD, ContextRequirements.Field.ACCESS_KEYS, false),
                new ContextRequirements.Field("storageUrl", "Manta Storage URL", ContextRequirements.FieldType.TEXT, false),
                new ContextRequirements.Field("proxyHost", "Proxy host", ContextRequirements.FieldType.TEXT, false),
                new ContextRequirements.Field("proxyPort", "Proxy port", ContextRequirements.FieldType.TEXT, false)
        );
    }
    
    @Override
    public @Nonnull String getCloudName() {
        return "Joyent Cloud";
    }
    
    @Override
    public @Nonnull JoyentDataCenter getDataCenterServices() {
        return new JoyentDataCenter(this);
    }
    
    static private final HashMap<String,Map<String,String>> endpointCache = new HashMap<String,Map<String,String>>();
    
    public @Nonnull String getEndpoint() throws CloudException, InternalException {
        ProviderContext ctx = getContext();

        if( ctx == null ) {
            throw new CloudException("No context has been established for this request");
        }
        String e = ctx.getEndpoint();
        
        if( e == null ) { 
            e = "https://us-west-1.api.joyentcloud.com";
        }
        String[] parts = e.split(",");
        
        if( parts == null || parts.length < 1 ) {
            parts = new String[] { e };
        }
        String r = ctx.getRegionId();
        
        if( r == null ) {
            return parts[0];
        }
        if( endpointCache.containsKey(e) ) {
            Map<String,String> cache = endpointCache.get(e);
            
            if( cache != null && cache.containsKey(r) ) {
                String endpoint = cache.get(r);
                
                if( endpoint != null ) {
                    return endpoint;
                }
            }
        }
        JoyentMethod method = new JoyentMethod(this);

        String json = method.doGetJson(parts[0], "datacenters");
        try {
            JSONObject ob = new JSONObject(json);
            JSONArray ids = ob.names();
            
            for( int i=0; i<ids.length(); i++ ) {
                String regionId = ids.getString(i);
                
                if( regionId.equals(r) && ob.has(regionId) ) {
                    String endpoint = ob.getString(regionId);
                    Map<String,String> cache;
                    
                    if( endpointCache.containsKey(e) ) {
                        cache = endpointCache.get(e);
                    }
                    else {
                        cache = new HashMap<String,String>();
                        endpointCache.put(e, cache);
                    }
                    cache.put(r, endpoint);
                    return endpoint;
                }
            }
            throw new CloudException("No endpoint exists for " + r);
        }
        catch( JSONException ex ) {
            throw new CloudException(ex);
        }        
    }
    
    @Override
    public @Nonnull String getProviderName() {
        return "Joyent";
    }
    
    public @Nonnegative long parseTimestamp(String time) throws CloudException {
        if( time == null ) {
            return 0L;
        }
        int idx = time.lastIndexOf('+');
        if (idx < 0) {
            idx = time.lastIndexOf('Z');
            if (idx < 0) {
                throw new CloudException("Could not parse timestamp: " + time);
            }
            time = time.substring(0,idx);
        } else {
            time = time.substring(0,idx);
        }
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        fmt.setTimeZone(TimeZone.getTimeZone("GMT"));
        if( time.length() > 0 ) {
            try {
                return fmt.parse(time).getTime();
            } catch( ParseException e ) {
                fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                try {
                    return fmt.parse(time).getTime();
                } catch (ParseException e2) {
                    throw new CloudException("Could not parse timestamp: " + time, e2);
                }
            }
        }
        return 0L;
    }
    
    @Override
    public @Nullable String testContext() {
        Logger logger = getLogger(SmartDataCenter.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + SmartDataCenter.class.getName() + ".textContext()");
        }
        try {
            try {
                ProviderContext ctx = getContext();

                if( ctx == null ) {
                    return null;
                }
                String pk = new String(ctx.getAccessPublic(), "utf-8");

                JoyentMethod method = new JoyentMethod(this);
                
                try {
                    method.doGetJson(getEndpoint(), "datacenters");
                    return pk;
                }
                catch( CloudException e ) {
                    if( e.getErrorType().equals(CloudErrorType.AUTHENTICATION) ) {
                        return null;
                    }
                    logger.warn("Cloud error testing Joyent context: " + e.getMessage(), e);
                }
                return null;
            }
            catch( Throwable t ) {
                logger.warn("Failed to test Joyent connection context: " + t.getMessage(), t);
                return null;
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + SmartDataCenter.class.getName() + ".testContext()");
            }
        }
    }
}
