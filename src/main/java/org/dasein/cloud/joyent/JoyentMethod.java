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

package org.dasein.cloud.joyent;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;

public class JoyentMethod {
    static public final String VERSION = "~6.5";
    
    protected SmartDataCenter provider;
    
    public JoyentMethod(@Nonnull SmartDataCenter provider) { this.provider = provider; }
    
    public void doDelete(@Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        Logger std = SmartDataCenter.getLogger(JoyentMethod.class, "std");
        Logger wire = SmartDataCenter.getLogger(JoyentMethod.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + JoyentMethod.class.getName() + ".doDelete(" + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("[" + (new Date()) + "] >>>----------------------------------------------------- [" + endpoint + "] [" + resource + "]");
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            DeleteMethod delete = new DeleteMethod(endpoint + "/my/" + resource);
            
            delete.addRequestHeader("Accept", "application/json");
            delete.addRequestHeader("X-Api-Version", "~6.5");
            delete.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("DELETE " + delete.getPath());
                for( Header header : delete.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(delete);
            }
            catch( IOException e ) {
                std.error("doDelete(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("doDelete(): HTTP Status " + code);
            }
            Header[] headers = delete.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(delete.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");                                
            }
            if( code != HttpServletResponse.SC_NO_CONTENT && code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_CREATED ) {
                std.error("doDelete(): Expected NO CONTENT for DELETE request, got " + code);
                String response;
                
                try {
                    response = delete.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("doDelete(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                JoyentException.ExceptionItems items = JoyentException.parseException(code, response);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("doDelete(): [" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                wire.debug("");
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + JoyentMethod.class.getName() + ".doDelete()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("[" + (new Date()) + "] -----------------------------------------------------<<< [" + endpoint + "] [" + resource + "]");
            }               
        }
    }
    
    public @Nullable String doGetJson(@Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        Logger std = SmartDataCenter.getLogger(JoyentMethod.class, "std");
        Logger wire = SmartDataCenter.getLogger(JoyentMethod.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + JoyentMethod.class.getName() + ".doGetJson(" + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("[" + (new Date()) + "] >>>----------------------------------------------------- [" + endpoint + "] [" + resource + "]");
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            GetMethod get = new GetMethod(endpoint + "/my/" + resource);

            get.addRequestHeader("Accept", "application/json");
            get.addRequestHeader("X-Api-Version", "~6.5");
            get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("GET " + get.getPath());
                for( Header header : get.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(get);
            }
            catch( IOException e ) {
                std.error("doGetJson(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("doGetJson(): HTTP Status " + code);
            }
            Header[] headers = get.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(get.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");                
            }
            if( code == HttpServletResponse.SC_NOT_FOUND ) {
                return null;
            }
            if( code != HttpServletResponse.SC_NO_CONTENT && code != HttpServletResponse.SC_OK && code != HttpServletResponse.SC_NON_AUTHORITATIVE_INFORMATION ) {
                std.error("doGetJson(): Expected OK for GET request, got " + code);
                String response;
                
                try {
                    response = get.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("doGetJson(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                JoyentException.ExceptionItems items = JoyentException.parseException(code, response);
                
                if( items == null ) {
                    return null;
                }
                std.error("getString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                String response;
                
                try {
                    response = get.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("getString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                return response;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + JoyentMethod.class.getName() + ".doGetJson()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("[" + (new Date()) + "] -----------------------------------------------------<<< [" + endpoint + "] [" + resource + "]");
            }               
        }
    }
    
    @SuppressWarnings("unused")
    public @Nullable InputStream doGetStream(@Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        Logger std = SmartDataCenter.getLogger(JoyentMethod.class, "std");
        Logger wire = SmartDataCenter.getLogger(JoyentMethod.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + JoyentMethod.class.getName() + ".doGetStream(" + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("[" + (new Date()) + "] >>>----------------------------------------------------- [" + endpoint + "] [" + resource + "]");
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            GetMethod get = new GetMethod(endpoint + "/my/" + resource);
            
            get.addRequestHeader("Accept", "application/json");
            get.addRequestHeader("X-Api-Version", "~6.5");
            get.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("GET " + get.getPath());
                for( Header header : get.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(get);
            }
            catch( IOException e ) {
                std.error("doGetStream(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("doGetStream(): HTTP Status " + code);
            }
            Header[] headers = get.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(get.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");                
            }
            if( code == HttpServletResponse.SC_NOT_FOUND ) {
                return null;
            }
            if( code != HttpServletResponse.SC_OK && code != HttpServletResponse.SC_NON_AUTHORITATIVE_INFORMATION ) {
                std.error("doGetStream(): Expected OK for GET request, got " + code);
                String response;
                
                try {
                    response = get.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("doGetStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                JoyentException.ExceptionItems items = JoyentException.parseException(code, response);
                
                if( items == null ) {
                    return null;
                }
                std.error("doGetStream(): [" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                InputStream input;
                
                try {
                    input = get.getResponseBodyAsStream();
                }
                catch( IOException e ) {
                    std.error("doGetStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug("---> Binary Data <---");
                }
                wire.debug("");
                return input;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + JoyentMethod.class.getName() + ".doGetStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("[" + (new Date()) + "] -----------------------------------------------------<<< [" + endpoint + "] [" + resource + "]");
            }               
        }
    }
    
    private @Nonnull HttpClient getClient() {
        Logger logger = SmartDataCenter.getLogger(JoyentMethod.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + JoyentMethod.class.getName() + ".getClient()");
        }
        try {
            ProviderContext ctx = provider.getContext();
            HttpClient client = new HttpClient();

            if( ctx != null ) {
                Properties p = ctx.getCustomProperties();

                if( p != null ) {
                    String proxyHost = p.getProperty("proxyHost");
                    String proxyPort = p.getProperty("proxyPort");

                    if( proxyHost != null ) {
                        int port = 0;

                        if( proxyPort != null && proxyPort.length() > 0 ) {
                            port = Integer.parseInt(proxyPort);
                        }
                        client.getHostConfiguration().setProxy(proxyHost, port);
                    }
                }
                try {
                    String username = new String(ctx.getAccessPublic(), "utf-8");
                    String password = new String(ctx.getAccessPrivate(), "utf-8");
                
                    client.getState().setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
                }
                catch( UnsupportedEncodingException e ) {
                    logger.error("getClient(): Invalid encoding: " + e.getMessage());
                    if( logger.isDebugEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new RuntimeException(e);
                }
            }
            return client;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + JoyentMethod.class.getName() + ".getClient()");
            }
        }
    }
    
    @SuppressWarnings("unused")
    public @Nullable String doPostHeaders(@Nonnull String endpoint, @Nonnull String resource, @Nullable Map<String,String> customHeaders) throws CloudException, InternalException {
        Logger std = SmartDataCenter.getLogger(JoyentMethod.class, "std");
        Logger wire = SmartDataCenter.getLogger(JoyentMethod.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + JoyentMethod.class.getName() + ".doPostHeaders(" + endpoint + "," + resource + "," + customHeaders + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("[" + (new Date()) + "] >>>----------------------------------------------------- [" + endpoint + "] [" + resource + "]");
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            PostMethod post = new PostMethod(endpoint + "/my/" + resource);
            
            post.addRequestHeader("Accept", "application/json");
            post.addRequestHeader("X-Api-Version", "~6.5");
            if( customHeaders != null ) {
                for( Map.Entry<String, String> entry : customHeaders.entrySet() ) {
                    String val = (entry.getValue() == null ? "" : entry.getValue());
                    
                    post.addRequestHeader(entry.getKey(), val);
                }
            }
            post.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("POST " + post.getPath());
                for( Header header : post.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                
            }
            int code;
            
            try {
                code = client.executeMethod(post);
            }
            catch( IOException e ) {
                std.error("doPostHeaders(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("doPostHeaders(): HTTP Status " + code);
            }
            Header[] headers = post.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT && code != HttpServletResponse.SC_CREATED ) {
                std.error("doPostHeaders(): Expected ACCEPTED for POST request, got " + code);
                String response;
                
                try {
                    response = post.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("doPostHeaders(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                JoyentException.ExceptionItems items = JoyentException.parseException(code, response);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("doPostHeaders(): [" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                if( code == HttpServletResponse.SC_ACCEPTED || code == HttpServletResponse.SC_CREATED ) {
                    String response;
                    
                    try {
                        response = post.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("doPostHeaders(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                        }
                        wire.debug("");
                        return response;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + JoyentMethod.class.getName() + ".doPostHeaders()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("[" + (new Date()) + "] -----------------------------------------------------<<< [" + endpoint + "] [" + resource + "]");
            }               
        }
    }
    
    public @Nullable String doPostString(@Nonnull String endpoint, @Nonnull String resource, @Nullable String payload) throws CloudException, InternalException {
        Logger std = SmartDataCenter.getLogger(JoyentMethod.class, "std");
        Logger wire = SmartDataCenter.getLogger(JoyentMethod.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + JoyentMethod.class.getName() + ".doPostString(" + endpoint + "," + resource + "," + payload + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("[" + (new Date()) + "] >>>----------------------------------------------------- [" + endpoint + "] [" + resource + "]");
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            PostMethod post = new PostMethod(endpoint + "/my/" + resource);
            
            if( payload != null && payload.startsWith("action") ) {
                post.addRequestHeader("Content-Type", "application/x-www-form-urlencoded");
            }
            else {
                post.addRequestHeader("Content-Type", "application/json");
            }
            post.addRequestHeader("Accept", "application/json");
            post.addRequestHeader("X-Api-Version", VERSION);
            post.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("POST " + post.getPath());
                for( Header header : post.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                
            }
            if( payload != null ) { 
                wire.debug(payload);
                wire.debug("");
                try {
                    if( payload.startsWith("action") ) {
                        post.setRequestEntity(new StringRequestEntity(payload, "application/x-www-form-urlencoded", "utf-8"));                        
                    }
                    else {
                        post.setRequestEntity(new StringRequestEntity(payload, "application/json", "utf-8"));
                    }
                }
                catch( UnsupportedEncodingException e ) {
                    std.error("doPostString(): UTF-8 is not supported locally: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new InternalException(e);
                }
            }
            int code;
            
            try {
                code = client.executeMethod(post);
            }
            catch( IOException e ) {
                std.error("doPostString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("doPostString(): HTTP Status " + code);
            }
            Header[] headers = post.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT && code != HttpServletResponse.SC_CREATED ) {
                std.error("doPostString(): Expected ACCEPTED for POST request, got " + code);
                String response;
                
                try {
                    response = post.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("doPostString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                JoyentException.ExceptionItems items = JoyentException.parseException(code, response);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("doPostString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                if( code == HttpServletResponse.SC_ACCEPTED || code == HttpServletResponse.SC_CREATED ) {
                    String response;
                    
                    try {
                        response = post.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("doPostString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                        }
                        wire.debug("");
                        return response;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + JoyentMethod.class.getName() + ".doPostString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("[" + (new Date()) + "] -----------------------------------------------------<<< [" + endpoint + "] [" + resource + "]");
            }               
        }
    }

    @SuppressWarnings("unused")
    public @Nullable String doPostStream(@Nonnull String endpoint, @Nonnull String resource, @Nullable String md5Hash, @Nullable InputStream stream) throws CloudException, InternalException {
        Logger std = SmartDataCenter.getLogger(JoyentMethod.class, "std");
        Logger wire = SmartDataCenter.getLogger(JoyentMethod.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + JoyentMethod.class.getName() + ".doPostStream(" + endpoint + "," + resource + "," + md5Hash + ",INPUTSTREAM)");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("[" + (new Date()) + "] >>>----------------------------------------------------- [" + endpoint + "] [" + resource + "]");
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            PostMethod post = new PostMethod(endpoint + resource);
            
            post.addRequestHeader("Content-Type", "application/octet-stream");
            post.addRequestHeader("Accept", "application/json");
            post.addRequestHeader("X-Api-Version", VERSION);
            post.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("POST " + post.getPath());
                for( Header header : post.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                
            }
            wire.debug("---> BINARY DATA <---");
            wire.debug("");
            post.setRequestEntity(new InputStreamRequestEntity(stream, "application/octet-stream"));
            int code;
            
            try {
                code = client.executeMethod(post);
            }
            catch( IOException e ) {
                std.error("doPostStream(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("doPostStream(): HTTP Status " + code);
            }
            Header[] headers = post.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            String responseHash = null;
            
            for( Header h : post.getResponseHeaders() ) {
                if( h.getName().equals("ETag") ) {
                    responseHash = h.getValue(); 
                }
            }
            if( responseHash != null && md5Hash != null && !responseHash.equals(md5Hash) ) {
                throw new CloudException("MD5 hash values do not match, probably data corruption");
            }
            if( code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT && code != HttpServletResponse.SC_CREATED ) {
                std.error("doPostStream(): Expected ACCEPTED or NO CONTENT for POST request, got " + code);
                String response;
                
                try {
                    response = post.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("doPostStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                }
                wire.debug("");
                JoyentException.ExceptionItems items = JoyentException.parseException(code, response);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("doPostStream(): [" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                wire.debug("");
                if( code == HttpServletResponse.SC_ACCEPTED || code == HttpServletResponse.SC_CREATED ) {
                    String response;
                    
                    try {
                        response = post.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("postStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                        }
                        wire.debug("");
                        return response;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + SmartDataCenter.class.getName() + ".doPostStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("[" + (new Date()) + "] -----------------------------------------------------<<< [" + endpoint + "] [" + resource + "]");
            }               
        }
    }
    
    @SuppressWarnings("unused")
    protected @Nullable String putHeaders(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nullable Map<String,String> customHeaders) throws CloudException, InternalException {
        Logger std = SmartDataCenter.getLogger(SmartDataCenter.class, "std");
        Logger wire = SmartDataCenter.getLogger(SmartDataCenter.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + JoyentMethod.class.getName() + ".putHeaders(" + authToken + "," + endpoint + "," + resource + "," + customHeaders + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            PutMethod put = new PutMethod(endpoint + resource);
            
            put.addRequestHeader("Content-Type", "application/json");
            put.addRequestHeader("Accept", "application/json");
            put.addRequestHeader("X-Auth-Token", authToken);
            if( customHeaders != null ) {
                for( Map.Entry<String, String> entry : customHeaders.entrySet() ) {
                    String val = (entry.getValue() == null ? "" : entry.getValue());
                    
                    put.addRequestHeader(entry.getKey(), val);
                }
            }
            put.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("PUT " + put.getPath());
                for( Header header : put.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            int code;
            
            try {
                code = client.executeMethod(put);
            }
            catch( IOException e ) {
                std.error("putString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("putString(): HTTP Status " + code);
            }
            Header[] headers = put.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(put.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpServletResponse.SC_CREATED && code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("putString(): Expected CREATED, ACCEPTED, or NO CONTENT for put request, got " + code);
                String response;
                
                try {
                    response = put.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("putString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                JoyentException.ExceptionItems items = JoyentException.parseException(code, response);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("putString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                if( code == HttpServletResponse.SC_ACCEPTED || code == HttpServletResponse.SC_CREATED ) {
                    String response;
                    
                    try {
                        response = put.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("putString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                            wire.debug("");
                        }
                        return response;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + JoyentMethod.class.getName() + ".putString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }
    
    @SuppressWarnings("unused")
    protected String putString(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nullable String payload) throws CloudException, InternalException {
        Logger std = SmartDataCenter.getLogger(SmartDataCenter.class, "std");
        Logger wire = SmartDataCenter.getLogger(SmartDataCenter.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + JoyentMethod.class.getName() + ".putString(" + authToken + "," + endpoint + "," + resource + "," + payload + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            PutMethod put = new PutMethod(endpoint + resource);
            
            put.addRequestHeader("Content-Type", "application/json");
            put.addRequestHeader("Accept", "application/json");
            put.addRequestHeader("X-Auth-Token", authToken);
            put.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("PUT " + put.getPath());
                for( Header header : put.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            if( payload != null ) { 
                wire.debug(payload);
                wire.debug("");
                try {
                    put.setRequestEntity(new StringRequestEntity(payload, "application/json", "utf-8"));
                }
                catch( UnsupportedEncodingException e ) {
                    std.error("putString(): UTF-8 is not supported locally: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new InternalException(e);
                }
            }
            int code;
            
            try {
                code = client.executeMethod(put);
            }
            catch( IOException e ) {
                std.error("putString(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("putString(): HTTP Status " + code);
            }
            Header[] headers = put.getResponseHeaders();
            
            if( wire.isDebugEnabled() ) {
                wire.debug(put.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            if( code != HttpServletResponse.SC_CREATED && code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("putString(): Expected CREATED, ACCEPTED, or NO CONTENT for put request, got " + code);
                String response;
                
                try {
                    response = put.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("putString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                JoyentException.ExceptionItems items = JoyentException.parseException(code, response);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("putString(): [" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                if( code == HttpServletResponse.SC_ACCEPTED || code == HttpServletResponse.SC_CREATED ) {
                    String response;
                    
                    try {
                        response = put.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("putString(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                            wire.debug("");
                        }
                        return response;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + JoyentMethod.class.getName() + ".putString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }

    @SuppressWarnings("unused")
    protected @Nullable String putStream(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nullable String md5Hash, @Nullable InputStream stream) throws CloudException, InternalException {
        Logger std = SmartDataCenter.getLogger(SmartDataCenter.class, "std");
        Logger wire = SmartDataCenter.getLogger(SmartDataCenter.class, "wire");
        
        if( std.isTraceEnabled() ) {
            std.trace("enter - " + JoyentMethod.class.getName() + ".putStream(" + authToken + "," + endpoint + "," + resource + "," + md5Hash + ",INPUTSTREAM)");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            wire.debug("");
        }
        try {
            HttpClient client = getClient();
            PutMethod put = new PutMethod(endpoint + resource);
            
            put.addRequestHeader("Content-Type", "application/octet-stream");
            put.addRequestHeader("Accept", "application/json");
            put.addRequestHeader("X-Auth-Token", authToken);
            if( md5Hash != null ) {
                put.addRequestHeader("ETag", md5Hash);
            }
            put.getParams().setCookiePolicy(CookiePolicy.IGNORE_COOKIES);
            if( wire.isDebugEnabled() ) {
                wire.debug("PUT " + put.getPath());
                for( Header header : put.getRequestHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
                wire.debug("---> BINARY DATA <---");
                wire.debug("");                
            }

            put.setRequestEntity(new InputStreamRequestEntity(stream, "application/octet-stream"));
            int code;
            
            try {
                code = client.executeMethod(put);
            }
            catch( IOException e ) {
                std.error("putStream(): Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                if( std.isTraceEnabled() ) {
                    e.printStackTrace();
                }
                throw new CloudException(e);
            }
            if( std.isDebugEnabled() ) {
                std.debug("putStream(): HTTP Status " + code);
            }
            Header[] headers = put.getResponseHeaders();

            if( wire.isDebugEnabled() ) {
                wire.debug(put.getStatusLine().toString());
                for( Header h : headers ) {
                    if( h.getValue() != null ) {
                        wire.debug(h.getName() + ": " + h.getValue().trim());
                    }
                    else {
                        wire.debug(h.getName() + ":");
                    }
                }
                wire.debug("");
            }
            String responseHash = null;
            
            for( Header h : put.getResponseHeaders() ) {
                if( h.getName().equals("ETag") ) {
                    responseHash = h.getValue(); 
                }
            }
            if( responseHash != null && md5Hash != null && !responseHash.equals(md5Hash) ) {
                throw new CloudException("MD5 hash values do not match, probably data corruption");
            }
            if( code != HttpServletResponse.SC_CREATED && code != HttpServletResponse.SC_ACCEPTED && code != HttpServletResponse.SC_NO_CONTENT ) {
                std.error("putStream(): Expected CREATED, ACCEPTED, or NO CONTENT for PUT request, got " + code);
                String response;
                
                try {
                    response = put.getResponseBodyAsString();
                }
                catch( IOException e ) {
                    std.error("putStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    if( std.isTraceEnabled() ) {
                        e.printStackTrace();
                    }
                    throw new CloudException(e);                    
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug(response);
                    wire.debug("");
                }
                JoyentException.ExceptionItems items = JoyentException.parseException(code, response);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                std.error("putStream(): [" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                if( code == HttpServletResponse.SC_ACCEPTED ) {
                    String response;
                    
                    try {
                        response = put.getResponseBodyAsString();
                    }
                    catch( IOException e ) {
                        std.error("putStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        if( std.isTraceEnabled() ) {
                            e.printStackTrace();
                        }
                        throw new CloudException(e);                    
                    }
                    if( response != null && !response.trim().equals("") ) {
                        if( wire.isDebugEnabled() ) {
                            wire.debug(response);
                            wire.debug("");
                        }
                        return response;
                    }
                }
                return null;
            }
        }
        finally {
            if( std.isTraceEnabled() ) {
                std.trace("exit - " + SmartDataCenter.class.getName() + ".putStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug("---------------------------------------------------------------------------------" + endpoint + resource);
            }               
        }
    }
}
