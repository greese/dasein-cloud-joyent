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

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VmStatistics;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.joyent.JoyentMethod;
import org.dasein.cloud.joyent.SmartDataCenter;
import org.dasein.util.CalendarWrapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Machine implements VirtualMachineSupport {
    private SmartDataCenter provider;
    
    Machine(SmartDataCenter sdc) { provider = sdc; }
    
    @Override
    public void boot(@Nonnull String vmId) throws InternalException, CloudException {
        JoyentMethod method = new JoyentMethod(provider);
        
        method.doPostString(provider.getEndpoint(), "machines/" + vmId, "action=start");
    }

    @Override
    public @Nonnull VirtualMachine clone(@Nonnull String vmId, @Nullable String intoDcId, @Nonnull String name, @Nonnull String description, boolean powerOn, @Nullable String... firewallIds) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Cloning is not currently suported");
    }

    @Override
    public void disableAnalytics(@Nonnull String vmId) throws InternalException, CloudException {
        // NO-OP
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
            // TODO: uncomment this bit when machine image support is in place
            if( !provider.getComputeServices().hasImageSupport() ) {
                vm.setArchitecture(Architecture.I64);
                vm.setPlatform(Platform.UNKNOWN);
                return;
            }
            MachineImage img = provider.getComputeServices().getImageSupport().getMachineImage(miId);
            
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
    
    @Override
    public void enableAnalytics(@Nonnull String vmId) throws InternalException, CloudException {
        // NO-OP
    }

    @Override
    public @Nonnull String getConsoleOutput(@Nonnull String vmId) throws InternalException, CloudException {
        return "";
    }

    static private HashMap<String,VirtualMachineProduct> productCache = new HashMap<String,VirtualMachineProduct>();
    
    @Override
    public @Nullable VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        if( productCache.containsKey(productId) ) {
            return productCache.get(productId);
        }
        for( VirtualMachineProduct prd : listProducts(Architecture.I64) ) {
            if( prd.getProductId().equals(productId) ) {
                productCache.put(productId, prd);
                return prd;
            }
        }
        return null;
    }

    @Override
    public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
        return "machine";
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
    public @Nullable VmStatistics getVMStatistics(@Nonnull String vmId, long from, long to) throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull Iterable<VmStatistics> getVMStatisticsForPeriod(@Nonnull String vmId, long from, long to) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        JoyentMethod method = new JoyentMethod(provider);
        
        method.doGetJson(provider.getEndpoint(), "packages");
        return true;
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nullable String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, @Nullable String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String... firewallIds) throws InternalException, CloudException {
        return launch(fromMachineImageId, product, dataCenterId, name, description, withKeypairId, inVlanId, withAnalytics, asSandbox, firewallIds, new Tag[0]);
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nullable String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, @Nullable String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String[] firewallIds, @Nullable Tag... tags) throws InternalException, CloudException {
        JoyentMethod method = new JoyentMethod(provider);
        HashMap<String,Object> post = new HashMap<String,Object>();
        
        String startupScript = "#!/usr/bin/env sh\n"
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
                +"fi\n";
        // TODO: FOR DEV ONLY... REMOVE BEFORE PROD!
//        startupScript += "configfile='/opt/local/enstratus/ws/tomcat/webapps/ROOT/WEB-INF/classes/enstratus-webservices.cfg'\n"
//        		+"\n"
//                +"echo \"==> Updating config file for dev environment...\"\n"
//                +"out=$(sed -i \"s/environment=production/environment=staging/\" $configfile)\n"
//                +"if [[ $? -ne 0 ]] ; then\n"
//                +"  echo \"error updating enstratus environment config\"\n"
//                +"  exit 1\n"
//                +"fi\n"
//                +"out=$(sed -i \"s/#provisioningProxy=255.255.255.255:3302/provisioningProxy=75.101.177.183:3302/\" $configfile)\n"
//                +"if [[ $? -ne 0 ]] ; then\n"
//                +"  echo \"error updating enstratus proxy config\"\n"
//                +"  exit 1\n"
//                +"fi\n"
//                +"\n"
//                +"svcadm restart enstratus";


        // Since Joyent stopped using meaningful naming conventions for SmartOS machines,
        // we'll need to just always try to run the user script to install our agent.  
        // That'll learn 'em.
    	post.put("metadata.user-script", startupScript);	

    	
    	name = validateName(name);
        post.put("name", name);
        post.put("package", product.getProductId());
        post.put("dataset", fromMachineImageId);

        //MachineImage img = provider.getComputeServices().getImageSupport().getMachineImage(fromMachineImageId);
        //if (img.getName().toLowerCase().contains("smartos")) {
        //	post.put("metadata.user-script", startupScript);
    	//}
        if( tags != null ) {
            for( Tag tag : tags ) {
                post.put("metadata." + tag.getKey(), tag.getValue());
            }
        }
        post.put("metadata.dsnTrueImage", fromMachineImageId);
        post.put("metadata.dsnTrueProduct", product.getProductId());
        post.put("metadata.dsnDescription", description);
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

    @Override
    public @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<VirtualMachineProduct> listProducts(@Nonnull Architecture architecture) throws InternalException, CloudException {
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
                    prd.setRamInMb(ob.getInt("memory"));
                }
                if( ob.has("disk") ) {
                    prd.setDiskSizeInGb(ob.getInt("disk")/1024);
                }
                prd.setCpuCount(1);
                prd.setDescription(prd.getName());
                prd.setProductId(prd.getName());
                products.add(prd);
            }
            return products;
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
    public void pause(@Nonnull String vmId) throws InternalException, CloudException {
        JoyentMethod method = new JoyentMethod(provider);
        
        method.doPostString(provider.getEndpoint(), "machines/" + vmId, "action=stop");
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        JoyentMethod method = new JoyentMethod(provider);
        
        method.doPostString(provider.getEndpoint(), "machines/" + vmId, "action=reboot");
    }

    @Override
    public boolean supportsAnalytics() throws CloudException, InternalException {
        return false;
    }

    @Override
    public void terminate(@Nonnull String vmId) throws InternalException, CloudException {
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
        while( !VmState.PAUSED.equals(currentState) && System.currentTimeMillis() < timeout ) {
            try { Thread.sleep(10000); }
            catch( InterruptedException e ) { /* ignore */ }
            vm = getVirtualMachine(vmId);
            if( vm == null ) {
                return;
            }
            currentState = vm.getCurrentState();
        }
        method.doDelete(provider.getEndpoint(), "machines/" + vmId);
    }

    private VirtualMachine toVirtualMachine(JSONObject ob) throws CloudException, InternalException {
        if( ob == null ) {
            return null;
        }
        Logger logger = SmartDataCenter.getLogger(Machine.class, "std");
        
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
                            vm.setProduct(getProduct(md.getString(name)));
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
            vm.setPausable(false);
            vm.setRebootable(false);
            if( ob.has("state") ) {
                String s = ob.getString("state");
                
                if( s.equalsIgnoreCase("running") ) {
                    vm.setCurrentState(VmState.RUNNING);
                    vm.setPausable(true);
                    vm.setRebootable(true);
                }
                else if( s.equalsIgnoreCase("provisioning") ) {
                    vm.setCurrentState(VmState.PENDING);
                }
                else if( s.equalsIgnoreCase("stopping") ) {
                    vm.setCurrentState(VmState.STOPPING);
                }
                else if( s.equalsIgnoreCase("stopped") ) {
                    vm.setCurrentState(VmState.PAUSED);
                }
                else if( s.equalsIgnoreCase("deleted") ) {
                    vm.setCurrentState(VmState.TERMINATED);
                }
                else {
                    logger.warn("toVirtualMachine(): Unknown VM state: " + s);
                    vm.setCurrentState(VmState.PENDING);
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
            if( vm.getProduct() == null ) {
                VirtualMachineProduct d = null;
                int disk, ram;
                
                disk = ob.getInt("disk")/1024;
                ram = ob.getInt("memory");
                for( VirtualMachineProduct prd : listProducts(vm.getArchitecture()) ) {
                    d = prd;
                    if( prd.getDiskSizeInGb() == disk && prd.getRamInMb() == ram ) {
                        vm.setProduct(prd);
                        break;
                    }
                }
                if( vm.getProduct() == null ) {
                    vm.setProduct(d);
                }
            }
            return vm;
        }
        catch( JSONException e ) {
            throw new CloudException(e);
        }
    }

    static private HashMap<String,String> urnMapping = new HashMap<String,String>();
    
    private String getImageIdFromUrn(String urn) throws CloudException, InternalException {
        Logger logger = SmartDataCenter.getLogger(Machine.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER: " + Machine.class.getName() + ".getImageIdFromUrn(" + urn + ")");
        }
        try {
            if( urnMapping.containsKey(urn) ) {
                return urnMapping.get(urn);
            }
            System.out.println("");
            System.out.println("URN: " + urn);
            MachineImage img = provider.getComputeServices().getImageSupport().getMachineImage(urn);

            if( img != null ) {
                String id = img.getProviderMachineImageId();

                System.out.println("ID: " + id);
                if( id != null ) {
                    urnMapping.put(urn, id);
                    System.out.println("");
                    return id;
                }
            }
            System.out.println("Unable to identify ID from URN: " + urn);
            System.out.println("");
            return urn;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT: " + Machine.class.getName() + ".getImageIdFromUrn()");
            }
        }
    }
    
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
