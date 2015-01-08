/**
 * Copyright (C) 2009-2014 Dell, Inc
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

import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.joyent.JoyentException;
import org.dasein.cloud.joyent.JoyentMethod;
import org.dasein.cloud.joyent.SmartDataCenter;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
import org.dasein.util.uom.time.TimePeriodUnit;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Machine extends AbstractVMSupport<SmartDataCenter> {
    Logger logger = SmartDataCenter.getLogger(Machine.class, "std");

    private SmartDataCenter provider;
    private transient volatile MachineCapabilities capabilities;

    Machine(SmartDataCenter sdc) {
        super(sdc);
        provider = sdc;
    }

    @Override
    /**
     * Only resizing of type=smartmachine is supported.
     */
    public VirtualMachine alterVirtualMachineProduct( @Nonnull String virtualMachineId, @Nonnull String productId ) throws InternalException, CloudException {
        JoyentMethod method = new JoyentMethod(provider);

        method.doPostString(provider.getEndpoint(), "machines/" + virtualMachineId, "action=resize&package="+productId);
        return getVirtualMachine(virtualMachineId);
    }


    @Override
    public void start(@Nonnull String vmId) throws InternalException, CloudException {
        JoyentMethod method = new JoyentMethod(provider);
        
        method.doPostString(provider.getEndpoint(), "machines/" + vmId, "action=start");
    }

    static private class MiData {
        public Architecture architecture;
        public Platform platform;
    }
    
    static private HashMap<String,MiData> miCache = new HashMap<String,MiData>();
    
    private void discover(@Nonnull VirtualMachine vm) throws InternalException, CloudException {
        String miId = vm.getProviderMachineImageId();
        
        if( miCache.containsKey(miId) ) {
            MiData d = miCache.get(miId);
            
            vm.setArchitecture(d.architecture);
            vm.setPlatform(d.platform);
        }
        else {
            if( !provider.getComputeServices().hasImageSupport() ) {
                vm.setArchitecture(Architecture.I64);
                vm.setPlatform(Platform.UNKNOWN);
                return;
            }
            MachineImage img = provider.getComputeServices().getImageSupport().getImage(miId);
            
            if( img == null ) {
                vm.setArchitecture(Architecture.I64);
                vm.setPlatform(Platform.UNKNOWN);
            }
            else {
                if( img.getName().contains("64") ) {
                    vm.setArchitecture(Architecture.I64);
                }
                else if( img.getName().contains("32") ) {
                    vm.setArchitecture(Architecture.I32);
                }
                else {
                    vm.setArchitecture(Architecture.I64);
                }
                vm.setPlatform(Platform.guess(img.getName() + " " + img.getDescription()));
                MiData d = new MiData();
                
                d.architecture = vm.getArchitecture();
                d.platform = vm.getPlatform();
                miCache.put(miId, d);
            }
        }
    }

    static private HashMap<String,VirtualMachineProduct> productCache = new HashMap<String,VirtualMachineProduct>();

    @Nonnull
    @Override
    public VirtualMachineCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new MachineCapabilities(provider);
        }
        return capabilities;
    }

    @Override
    public @Nullable VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        if( productCache.containsKey(productId) ) {
            return productCache.get(productId);
        }
        for( VirtualMachineProduct prd : listProducts(Architecture.I64) ) {
            if( prd.getProviderProductId().equals(productId) ) {
                productCache.put(productId, prd);
                return prd;
            }
        }
        return null;
    }

    @Override
    public @Nullable VirtualMachine getVirtualMachine(@Nonnull String vmId) throws InternalException, CloudException {
        JoyentMethod method = new JoyentMethod(provider);

        try {
            String json = method.doGetJson(provider.getEndpoint(), "machines/" + vmId);
            
            if( json == null ) {
                return null;
            }
            return toVirtualMachine(new JSONObject(json));
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {

        org.dasein.cloud.util.Cache<Boolean> cache = org.dasein.cloud.util.Cache.getInstance(getProvider(),
                "isSubscribedVirtualMachine", Boolean.class, CacheLevel.REGION_ACCOUNT);
        final Iterable<Boolean> cachedIsSubscribed = cache.get(getContext());
        if (cachedIsSubscribed != null && cachedIsSubscribed.iterator().hasNext()) {
            final Boolean isSubscribed = cachedIsSubscribed.iterator().next();
            if (isSubscribed != null) {
                return isSubscribed;
            }
        }

        JoyentMethod method = new JoyentMethod(provider);
        try {
            method.doGetJson(provider.getEndpoint(), "packages");
        } catch (JoyentException e) {
            if (e.getErrorType().equals(CloudErrorType.AUTHENTICATION)) {
                cache.put(getContext(), Collections.singleton(false));
                return false;
            }
            throw e;
        }
        cache.put(getContext(), Collections.singleton(true));
        return true;
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull VMLaunchOptions withLaunchOptions) throws CloudException, InternalException {
        JoyentMethod method = new JoyentMethod(provider);
        Map<String, Object> post = new HashMap<String,Object>();

        if( withLaunchOptions.getUserData() != null ) {
            post.put("metadata.user-script", withLaunchOptions.getUserData());
        }
        if( withLaunchOptions.getHostName() != null ) {
            String name = validateName(withLaunchOptions.getHostName());

            post.put("name", name);
        }
        // Joyent will use datacenter default package/dataset if missing
        if( withLaunchOptions.getStandardProductId() != null ) {
            post.put("package", withLaunchOptions.getStandardProductId());
        }
        if( withLaunchOptions.getMachineImageId() != null ) {
            post.put("dataset", withLaunchOptions.getMachineImageId());
        }

        Map<String, Object> meta = withLaunchOptions.getMetaData();

        if( meta.size() > 0 ) {
            for( Map.Entry<String,Object> entry : meta.entrySet() ) {
                post.put("metadata." + entry.getKey(), entry.getValue().toString());
            }
        }
        post.put("metadata.dsnTrueImage", withLaunchOptions.getMachineImageId());
        post.put("metadata.dsnTrueProduct", withLaunchOptions.getStandardProductId());
        post.put("metadata.dsnDescription", withLaunchOptions.getDescription());
        String json = method.doPostString(provider.getEndpoint(), "machines", new JSONObject(post).toString());

        if( json == null ) {
            throw new CloudException("No machine was created");
        }
        try {
            return toVirtualMachine(new JSONObject(json));
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nonnull String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, @Nullable String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String[] firewallIds, @Nullable Tag... tags) throws InternalException, CloudException {
        VMLaunchOptions options;

        if( inVlanId == null ) {
            options = VMLaunchOptions.getInstance(product.getProviderProductId(), fromMachineImageId, name, description).inDataCenter(dataCenterId);
        }
        else {
            options = VMLaunchOptions.getInstance(product.getProviderProductId(), fromMachineImageId, name, description).inVlan(null, dataCenterId, inVlanId);
        }
        if( withKeypairId != null ) {
            options = options.withBoostrapKey(withKeypairId);
        }
        if( tags != null ) {
            for( Tag t : tags ) {
                options = options.withMetaData(t.getKey(), t.getValue());
            }
        }
        if( firewallIds != null ) {
            options = options.behindFirewalls(firewallIds);
        }

        // this is backwards compat for some old enStratus stuff in Joyent
        // you really should be using the launch(VMLaunchOptions) and you won't be burdened
        // by this legacy stuff
        options = options.withUserData("#!/usr/bin/env sh\n"
                +"\n"
                +"export PATH=/opt/local/bin:/opt/local/sbin:/usr/bin:/usr/sbin\n"
                +"\n"
                +"if [[ $EUID -ne 0 ]] ; then\n"
                +"  echo \"Must be root to install enstratus package\"\n"
                +"  exit 1\n"
                +"fi\n"
                +"\n"
                +"echo \"==> Updating package lists...\"\n"
                +"out=$(pkgin update)\n"
                +"if [[ $? -ne 0 ]] ; then\n"
                +"  echo \"error updating package lists\"\n"
                +"  exit 1\n"
                +"fi\n"
                +"\n"
                +"echo \"==> Installing Sun-jdk6...\"\n"
                +"out=$(yes | pkgin in sun-jdk6 sun-jre6)\n"
                +"if [[ $? -ne 0 ]] ; then\n"
                +"  echo \"error installing java\"\n"
                +"  exit 1\n"
                +"fi\n"
                +"\n"
                +"echo \"==> Installing enstratus package...\"\n"
                +"out=$(pkgin -y in enstratus-agent)\n"
                +"if [[ $? -ne 0 ]] ; then\n"
                +"  echo \"error installing enstratus agent\"\n"
                +"  exit 1\n"
                +"fi\n");
        return launch(options);
    }

    @Override
    public @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId) throws InternalException, CloudException {
        // TODO
        return Collections.emptyList();
    }



    @Override
    public @Nonnull Iterable<VirtualMachineProduct> listProducts(VirtualMachineProductFilterOptions options, Architecture architecture) throws InternalException, CloudException {
        Cache<VirtualMachineProduct> cache = Cache.getInstance(getProvider(), "VM.listProducts", VirtualMachineProduct.class, CacheLevel.REGION_ACCOUNT, TimePeriod.valueOf(1, "day"));
        final Iterable<VirtualMachineProduct> cachedProducts = cache.get(getContext());
        if( cachedProducts != null && cachedProducts.iterator().hasNext() ) {
            return cachedProducts;
        }
        JoyentMethod method = new JoyentMethod(provider);
        String json = method.doGetJson(provider.getEndpoint(), "packages");
        
        if( json == null ) {
            return Collections.emptyList();
        }
        try {
            ArrayList<VirtualMachineProduct> products = new ArrayList<VirtualMachineProduct>();
            JSONArray list = new JSONArray(json);

            for( int i=0; i<list.length(); i++ ) {
                JSONObject ob = list.getJSONObject(i);
                VirtualMachineProduct prd = new VirtualMachineProduct();
                
                if( ob.has("name") ) {
                    prd.setName(ob.getString("name"));
                }
                if( ob.has("memory") ) {
                    prd.setRamSize(new Storage<Megabyte>(ob.getInt("memory"), Storage.MEGABYTE));
                }
                if( ob.has("disk") ) {
                    prd.setRootVolumeSize(new Storage<Megabyte>(ob.getInt("disk"), Storage.MEGABYTE));
                }
                if( ob.has("vcpus") ) {
                    prd.setCpuCount(ob.getInt("vcpus"));
                }
                // SmartOS products are returned with 0 vCPUs as this metric doesn't apply
                // to them according to Joyent. In SmartOS you get some burstable capacity.
                // We will set them to 1 CPU anyway, as zero CPU is no CPU.
                if( prd.getCpuCount() == 0 ) {
                    prd.setCpuCount(1);
                }
                if( ob.has("description") ) {
                    prd.setDescription(ob.getString("description"));
                }
                else {
                    prd.setDescription(prd.getName());
                }
                prd.setProviderProductId(ob.getString("id"));
                if( options != null) {
                    if( options.matches(prd) )
                        products.add(prd);
                }
                else {
                    products.add(prd);
                }
            }
            cache.put(getContext(), products);
            return products;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVirtualMachineStatus() throws InternalException, CloudException {
        JoyentMethod method = new JoyentMethod(provider);

        try {
            JSONArray machines = new JSONArray(method.doGetJson(provider.getEndpoint(), "machines"));
            ArrayList<ResourceStatus> vms = new ArrayList<ResourceStatus>();

            for( int i=0; i<machines.length(); i++ ) {
                ResourceStatus vm = toStatus(machines.getJSONObject(i));

                if( vm != null ) {
                    vms.add(vm);
                }
            }
            return vms;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    @Override
    public @Nonnull Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        JoyentMethod method = new JoyentMethod(provider);

        try {
            JSONArray machines = new JSONArray(method.doGetJson(provider.getEndpoint(), "machines"));
            ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();
    
            for( int i=0; i<machines.length(); i++ ) {
                VirtualMachine vm = toVirtualMachine(machines.getJSONObject(i));
                
                if( vm != null ) {
                    vms.add(vm);
                }
            }
            return vms;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void stop(@Nonnull String vmId, boolean force) throws InternalException, CloudException {
        JoyentMethod method = new JoyentMethod(provider);

        method.doPostString(provider.getEndpoint(), "machines/" + vmId, "action=stop");
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        JoyentMethod method = new JoyentMethod(provider);
        
        method.doPostString(provider.getEndpoint(), "machines/" + vmId, "action=reboot");
    }

    @Override
    public void terminate(@Nonnull String vmId, @Nullable String explanation) throws InternalException, CloudException {
        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 20);
        JoyentMethod method = new JoyentMethod(provider);
        VirtualMachine vm = getVirtualMachine(vmId);
        
        if( vm == null ) {
            throw new CloudException("No such server: " + vmId);
        }
        VmState currentState = vm.getCurrentState();
        while( VmState.PENDING.equals(currentState) && System.currentTimeMillis() < timeout ) {
            try { Thread.sleep(10000); }
            catch( InterruptedException e ) { /* ignore */ }
            vm = getVirtualMachine(vmId);
            if( vm == null ) {
                return;
            }
            currentState = vm.getCurrentState();
        }
        if( VmState.RUNNING.equals(currentState) ) {
            method.doPostString(provider.getEndpoint(), "machines/" + vmId, "action=stop");
        }
        while( !VmState.STOPPED.equals(currentState) && !VmState.TERMINATED.equals(currentState) && System.currentTimeMillis() < timeout ) {
            try { Thread.sleep(10000); }
            catch( InterruptedException e ) { /* ignore */ }
            vm = getVirtualMachine(vmId);
            if( vm == null ) {
                return;
            }
            currentState = vm.getCurrentState();
        }
        method.doDelete(provider.getEndpoint(), "machines/" + vmId);
        if( timeout < CalendarWrapper.MINUTE * 5 ) {
            timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 5);
        }
        while( System.currentTimeMillis() < timeout ) {
            if( vm == null || VmState.TERMINATED.equals(vm.getCurrentState()) ) {
                return;
            }
            try { Thread.sleep(10000L); }
            catch( InterruptedException ignore ) { }
            vm = getVirtualMachine(vmId);
        }
        logger.warn("System timed out waiting for VM termination");
    }

    @Override
    public @Nullable String getPassword( @Nonnull String vmId ) throws InternalException, CloudException {
        final String[] adminUsernames = {"root", "administrator", "admin"};
        JoyentMethod method = new JoyentMethod(provider);
        try {
            JSONObject ob = new JSONObject(method.doGetJson(provider.getEndpoint(), "machines/"+vmId+"/metadata?credentials=true"));
            if( ob.has("credentials") ) {
                JSONObject credentials = ob.getJSONObject("credentials");
                for( String username : adminUsernames ) {
                    if( ob.has(username) ) {
                        return ob.getString(username);
                    }
                }
            }
        } catch( JSONException e ) {
            throw new InternalException("Unable to parse the response", e);
        }
        return null;
    }

    @Override
    public @Nullable String getUserData( @Nonnull String vmId ) throws InternalException, CloudException {
        JoyentMethod method = new JoyentMethod(provider);
        try {
            JSONObject ob = new JSONObject(method.doGetJson(provider.getEndpoint(), "machines/"+vmId+"/metadata"));
            if( ob.has("user-script") ) {
                return ob.getString("user-script");
            }
        } catch( JSONException e ) {
            throw new InternalException("Unable to parse the response", e);
        }
        return null;
    }

    private VirtualMachine toVirtualMachine(JSONObject ob) throws CloudException, InternalException {
        if( ob == null ) {
            return null;
        }
        try {
            VirtualMachine vm = new VirtualMachine();
            
            vm.setClonable(false);
            vm.setImagable(false);
            vm.setLastPauseTimestamp(-1L);
            vm.setPersistent(true);
            vm.setProviderDataCenterId(provider.getContext().getRegionId() + "a");
            vm.setProviderOwnerId(provider.getContext().getAccountNumber());
            vm.setProviderRegionId(provider.getContext().getRegionId());
            vm.setTerminationTimestamp(-1L);

            if(ob.has("id") ) {
                vm.setProviderVirtualMachineId(ob.getString("id"));
            }
            if( ob.has("name") ) {
                vm.setName(ob.getString("name"));
            }
            if( ob.has("ips") ) {
                JSONArray ips = ob.getJSONArray("ips");
                ArrayList<String> pubIp = new ArrayList<String>();
                ArrayList<String> privIp = new ArrayList<String>();
                
                for( int i=0; i<ips.length(); i++ ) {
                    String addr = ips.getString(i);
                    boolean pub = false;
                    
                    if( !addr.startsWith("10.") && !addr.startsWith("192.168.") ) {
                        if( addr.startsWith("172.") ) {
                            String[] nums = addr.split("\\.");
                            
                            if( nums.length != 4 ) {
                                pub = true;
                            }
                            else {
                                try {
                                    int x = Integer.parseInt(nums[1]);
                                    
                                    if( x < 16 || x > 31 ) {
                                        pub = true;
                                    }
                                }
                                catch( NumberFormatException ignore ) {
                                    // ignore
                                }
                            }
                        }
                        else {
                            pub = true;
                        }
                    }
                    if( pub ) {
                        pubIp.add(addr);
                    }
                    else {
                        privIp.add(addr);
                    }
                }
                if( !pubIp.isEmpty() ) {
                    vm.setPublicIpAddresses(pubIp.toArray(new String[pubIp.size()]));
                }
                if( !privIp.isEmpty() ) {
                    vm.setPrivateIpAddresses(privIp.toArray(new String[privIp.size()]));
                }
            }
            if( ob.has("metadata") ) {
                JSONObject md = ob.getJSONObject("metadata");
                JSONArray names = md.names();
                
                if( names != null ) {
                    for( int i=0; i<names.length(); i++ ) {
                        String name = names.getString(i);
    
                        if( name.equals("dsnDescription") ) {
                            vm.setDescription(md.getString(name));
                        }
                        else if( name.equals("dsnTrueImage") ) {
                            vm.setProviderMachineImageId(md.getString(name));
                        }
                        else if( name.equals("dsnTrueProduct") ) {
                            vm.setProductId(md.getString(name));
                        }
                        else {
                            vm.addTag(name, md.getString(name));
                        }
                    }
                }
            }
            if( vm.getProviderMachineImageId() == null && ob.has("dataset") ) {
                vm.setProviderMachineImageId(getImageIdFromUrn(ob.getString("dataset")));
            }
            if( ob.has("created") ) {
                vm.setCreationTimestamp(provider.parseTimestamp(ob.getString("created")));
            }
            vm.setPausable(false); // can't ever pause/resume joyent vms
            vm.setRebootable(false);
            if( ob.has("state") ) {
                vm.setCurrentState(toState(ob.getString("state")));

                if( VmState.RUNNING.equals(vm.getCurrentState()) ) {
                    vm.setRebootable(true);
                }
                else if( VmState.STOPPED.equals(vm.getCurrentState())) {
                    vm.setImagable(true);
                }
            }
            vm.setLastBootTimestamp(vm.getCreationTimestamp());
            if( vm.getName() == null ) {
                vm.setName(vm.getProviderVirtualMachineId());
            }
            if( vm.getDescription() == null ) {
                vm.setDescription(vm.getName());
            }
            discover(vm);
            boolean isVMSmartOs = (vm.getPlatform().equals(Platform.SMARTOS));
            if( vm.getProductId() == null ) {
                VirtualMachineProduct d = null;
                int disk, ram;
                
                disk = ob.getInt("disk");
                ram = ob.getInt("memory");
                for( VirtualMachineProduct prd : listProducts(vm.getArchitecture()) ) {
                    d = prd;
                    boolean isProductSmartOs = prd.getName().contains("smartos");
                    if( prd.getRootVolumeSize().convertTo(Storage.MEGABYTE).intValue() == disk && prd.getRamSize().intValue() == ram ) {
                        if (isVMSmartOs && !isProductSmartOs){
                            continue;
                        }
                        if (!isVMSmartOs && isProductSmartOs){
                            continue;
                        }
                        vm.setProductId(prd.getProviderProductId());
                        break;
                    }
                }
                if( vm.getProductId() == null ) {
                    vm.setProductId(d.getProviderProductId());
                }
            }
            return vm;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    private @Nonnull VmState toState(@Nonnull String s) {
        if( s.equalsIgnoreCase("running") ) {
            return VmState.RUNNING;
        }
        else if( s.equalsIgnoreCase("provisioning") ) {
            return VmState.PENDING;
        }
        else if( s.equalsIgnoreCase("stopping") ) {
            return VmState.STOPPING;
        }
        else if( s.equalsIgnoreCase("stopped") ) {
            return VmState.STOPPED;
        }
        else if( s.equalsIgnoreCase("deleted") ) {
            return VmState.TERMINATED;
        }
        else {
            logger.warn("DEBUG: Unknown Joyent VM state: " + s);
            return VmState.PENDING;
        }
    }

    private @Nullable ResourceStatus toStatus(@Nullable JSONObject ob) throws CloudException, InternalException {
        if( ob == null ) {
            return null;
        }
        try {
            VmState state = VmState.PENDING;
            String vmId = null;

            if(ob.has("id") ) {
                vmId = ob.getString("id");
            }
            if( ob.has("state") ) {
                state = toState(ob.getString("state"));
            }
            if( vmId == null ) {
                return null;
            }
            return new ResourceStatus(vmId, state);
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    static private HashMap<String,String> urnMapping = new HashMap<String,String>();
    
    private String getImageIdFromUrn(String urn) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER: " + Machine.class.getName() + ".getImageIdFromUrn(" + urn + ")");
        }
        try {
            if( urnMapping.containsKey(urn) ) {
                return urnMapping.get(urn);
            }
            MachineImage img = provider.getComputeServices().getImageSupport().getImage(urn);

            if( img != null ) {
                String id = img.getProviderMachineImageId();

                if( id != null ) {
                    urnMapping.put(urn, id);
                    return id;
                }
            }
            return urn;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT: " + Machine.class.getName() + ".getImageIdFromUrn()");
            }
        }
    }

    /*
    TODO: fix
    @see org.dasein.cloud.joyent.compute.MachineCapabilities#getVirtualMachineNamingConstraints
    */
    private String validateName(String originalName) {
        StringBuilder name = new StringBuilder();
        
        for( int i=0; i<originalName.length(); i++ ) {
            char c = originalName.charAt(i);
            
            if( (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ) {
                name.append(c);
            }
            else if( ((c >= '0' && c <= '9') || c == '-' || c == '_' || c == ' ') && name.length() > 0 ) {
                if( c == ' ' ) {
                    c = '-';
                }
                name.append(c);
            }
        }
        if( name.length() < 1 ) {
            return "unnamed-" + System.currentTimeMillis();
        }
        if ( name.charAt(name.length()-1)=='-' || name.charAt(name.length()-1)=='_') {  // check for trailing - or _
        	name.deleteCharAt(name.length()-1);
        }
        return name.toString();
    }
}
