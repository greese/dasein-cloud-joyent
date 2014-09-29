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

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.security.joyent.*;

public class JoyentMethod {
    static private final Logger logger = SmartDataCenter.getLogger(JoyentMethod.class, "std");
    static private final Logger wire   = SmartDataCenter.getLogger(JoyentMethod.class, "wire");
    static private final ContentType APPLICATION_FORM_URLENCODED_UTF8 = ContentType.create("application/x-www-form-urlencoded", "UTF-8");
    static private final ContentType APPLICATION_JSON_UTF8 = ContentType.create("application/json", "UTF-8");        
    static public final String VERSION = "~7.1";
    
    private JoyentClientFactory clientFactory;
    private JoyentHttpAuth httpAuth;
    private RequestTrackingStrategy strategy;
    
    public JoyentMethod(@Nonnull SmartDataCenter provider) {
        this.clientFactory = new DefaultClientFactory(provider.getContext());
        this.httpAuth = new SignatureHttpAuth(provider);
        this.strategy = provider.getContext().getRequestTrackingStrategy();
    }
    
    public void doDelete(@Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + JoyentMethod.class.getName() + ".doDelete(" + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug(">>> [DELETE (" + (new Date()) + ")] -> " + endpoint + "/my/" + resource + " >--------------------------------------------------------------------------------------");
        }
        try {
            HttpClient client = clientFactory.getClient(endpoint);
            HttpDelete delete = new HttpDelete(endpoint + "/my/" + resource);
            httpAuth.addPreemptiveAuth(delete);

            delete.addHeader("Accept", "application/json");
            delete.addHeader("X-Api-Version", VERSION);
            if(strategy != null && strategy.getSendAsHeader()){
                delete.addHeader(strategy.getHeaderName(), strategy.getRequestID());
            }

            if( wire.isDebugEnabled() ) {
                wire.debug(delete.getRequestLine().toString());
                for( Header header : delete.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }

            HttpResponse response;

            try {
                response = client.execute(delete);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            logger.debug("HTTP STATUS: " + code);

            if( code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_CREATED ) {
                logger.error("Expected NO CONTENT for DELETE request, got " + code);
                String json = null;
                
                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        json = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    throw new CloudException(e);
                }
                JoyentException.ExceptionItems items = JoyentException.parseException(code, json);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                logger.error("[" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                wire.debug("");
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + JoyentMethod.class.getName() + ".doDelete()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("<<< [DELETE (" + (new Date()) + ")] -> " + endpoint + "/my/" + resource + " <--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }
    
    public @Nullable String doGetJson(@Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + JoyentMethod.class.getName() + ".doGetJson(" + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug(">>> [GET (" + (new Date()) + ")] -> " + endpoint + "/my/" + resource + " >--------------------------------------------------------------------------------------");
        }
        try {
            HttpClient client = clientFactory.getClient(endpoint);
            HttpGet get = new HttpGet(endpoint + "/my/" + resource);
            httpAuth.addPreemptiveAuth(get);

            get.addHeader("Accept", "application/json");
            get.addHeader("X-Api-Version", VERSION);
            if(strategy != null && strategy.getSendAsHeader()){
                get.addHeader(strategy.getHeaderName(), strategy.getRequestID());
            }

            if( wire.isDebugEnabled() ) {
                wire.debug(get.getRequestLine().toString());
                for( Header header : get.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(get);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            logger.debug("HTTP STATUS: " + code);

            if( code == HttpStatus.SC_NOT_FOUND || code == HttpStatus.SC_GONE ) {
                return null;
            }
            if( code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_OK && code != HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION ) {
                logger.error("Expected OK for GET request, got " + code);
                String json = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        json = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    throw new CloudException(e);
                }

                JoyentException.ExceptionItems items = JoyentException.parseException(code, json);
                
                if( items == null ) {
                    return null;
                }
                logger.error("[" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                String json = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        json = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    throw new CloudException(e);
                }
                return json;
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + JoyentMethod.class.getName() + ".doGetJson()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("<<< [GET (" + (new Date()) + ")] -> " + endpoint + "/my/" + resource + " <--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }
    
    public @Nullable InputStream doGetStream(@Nonnull String endpoint, @Nonnull String resource) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + JoyentMethod.class.getName() + ".doGetStream(" + endpoint + "," + resource + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug(">>> [GET (" + (new Date()) + ")] -> " + endpoint + "/my/" + resource + " >--------------------------------------------------------------------------------------");
        }
        try {
            HttpClient client = clientFactory.getClient(endpoint);
            HttpGet get = new HttpGet(endpoint + "/my/" + resource);
            httpAuth.addPreemptiveAuth(get);

            get.addHeader("Accept", "application/json");
            get.addHeader("X-Api-Version", VERSION);
            if(strategy != null && strategy.getSendAsHeader()){
                get.addHeader(strategy.getHeaderName(), strategy.getRequestID());
            }

            if( wire.isDebugEnabled() ) {
                wire.debug(get.getRequestLine().toString());
                for( Header header : get.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(get);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            logger.debug("HTTP STATUS: " + code);

            if( code == HttpStatus.SC_NOT_FOUND ) {
                return null;
            }
            if( code != HttpStatus.SC_OK && code != HttpStatus.SC_NON_AUTHORITATIVE_INFORMATION ) {
                logger.error("Expected OK for GET request, got " + code);
                String json = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        json = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    throw new CloudException(e);
                }
                JoyentException.ExceptionItems items = JoyentException.parseException(code, json);
                
                if( items == null ) {
                    return null;
                }
                logger.error("[" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                InputStream input;
                
                try {
                    HttpEntity entity = response.getEntity();

                    if( entity == null ) {
                        return null;
                    }
                    input = entity.getContent();
                }
                catch( IOException e ) {
                    logger.error("doGetStream(): Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    throw new CloudException(e);
                }
                if( wire.isDebugEnabled() ) {
                    wire.debug("---> Binary Data <---");
                    wire.debug("");
                }
                return input;
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + JoyentMethod.class.getName() + ".doGetStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("<<< [GET (" + (new Date()) + ")] -> " + endpoint + "/my/" + resource + " <--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }
    
    public @Nullable String doPostHeaders(@Nonnull String endpoint, @Nonnull String resource, @Nullable Map<String,String> customHeaders) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + JoyentMethod.class.getName() + ".doPostHeaders(" + endpoint + "," + resource +  "," + customHeaders + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug(">>> [POST (" + (new Date()) + ")] -> " + endpoint + "/my/" + resource + " >--------------------------------------------------------------------------------------");
        }
        try {
            HttpClient client = clientFactory.getClient(endpoint);
            HttpPost post = new HttpPost(endpoint + "/my/" + resource);
            httpAuth.addPreemptiveAuth(post);

            post.addHeader("Accept", "application/json");
            post.addHeader("X-Api-Version", VERSION);
            if( customHeaders != null ) {
                for( Map.Entry<String, String> entry : customHeaders.entrySet() ) {
                    String val = (entry.getValue() == null ? "" : entry.getValue());
                    
                    post.addHeader(entry.getKey(), val);
                }
            }
            if(strategy != null && strategy.getSendAsHeader()){
                post.addHeader(strategy.getHeaderName(), strategy.getRequestID());
            }

            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");

                try { wire.debug(EntityUtils.toString(post.getEntity())); }
                catch( IOException ignore ) { }

                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            logger.debug("HTTP STATUS: " + code);

            if( code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_CREATED ) {
                logger.error("Expected ACCEPTED for POST request, got " + code);
                String json = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        json = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    throw new CloudException(e);
                }

                JoyentException.ExceptionItems items = JoyentException.parseException(code, json);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                logger.error("[" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                if( code == HttpStatus.SC_ACCEPTED || code == HttpStatus.SC_CREATED ) {
                    String json = null;

                    try {
                        HttpEntity entity = response.getEntity();

                        if( entity != null ) {
                            json = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(json);
                                wire.debug("");
                            }
                        }
                    }
                    catch( IOException e ) {
                        logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if( json != null && !json.trim().equals("") ) {
                        return json;
                    }
                }
                return null;
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + JoyentMethod.class.getName() + ".doPostHeaders()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("<<< [POST (" + (new Date()) + ")] -> " + endpoint + "/my/" + resource + " <--------------------------------------------------------------------------------------");
                wire.debug("");
            }               
        }
    }
    
    public @Nullable String doPostString(@Nonnull String endpoint, @Nonnull String resource, @Nullable String payload) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + JoyentMethod.class.getName() + ".doPostString(" + endpoint + "," + resource +  ",PAYLOAD)");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug(">>> [POST (" + (new Date()) + ")] -> " + endpoint + "/my/" + resource + " >--------------------------------------------------------------------------------------");
        }
        try {
            HttpClient client = clientFactory.getClient(endpoint);
            HttpPost post = new HttpPost(endpoint + "/my/" + resource);
            httpAuth.addPreemptiveAuth(post);

            if( payload != null && payload.startsWith("action") ) {
                post.addHeader("Content-Type", "application/x-www-form-urlencoded");
            }
            else {
                post.addHeader("Content-Type", "application/json");
            }
            post.addHeader("Accept", "application/json");
            post.addHeader("X-Api-Version", VERSION);
            if(strategy != null && strategy.getSendAsHeader()){
                post.addHeader(strategy.getHeaderName(), strategy.getRequestID());
            }

            if( payload != null && payload.startsWith("action") ) {
                post.setEntity(new StringEntity(payload, APPLICATION_FORM_URLENCODED_UTF8));
            }
            else {
                post.setEntity(new StringEntity(payload == null ? "" : payload, APPLICATION_JSON_UTF8));
            }
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");

                try { wire.debug(EntityUtils.toString(post.getEntity())); }
                catch( IOException ignore ) { }

                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            logger.debug("HTTP STATUS: " + code);

            if( code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_CREATED ) {
                logger.error("Expected ACCEPTED for POST request, got " + code);
                String json = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        json = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    throw new CloudException(e);
                }

                JoyentException.ExceptionItems items = JoyentException.parseException(code, json);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                logger.error("[" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                if( code == HttpStatus.SC_ACCEPTED || code == HttpStatus.SC_CREATED ) {
                    String json = null;

                    try {
                        HttpEntity entity = response.getEntity();

                        if( entity != null ) {
                            json = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(json);
                                wire.debug("");
                            }
                        }
                    }
                    catch( IOException e ) {
                        logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if( json != null && !json.trim().equals("") ) {
                        return json;
                    }
                }
                return null;
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + JoyentMethod.class.getName() + ".doPostString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("<<< [POST (" + (new Date()) + ")] -> " + endpoint + "/my/" + resource + " <--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }

    public @Nullable String doPostStream(@Nonnull String endpoint, @Nonnull String resource, @Nullable String md5Hash, @Nullable InputStream stream) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + JoyentMethod.class.getName() + ".doPostStream(" + endpoint + "," + resource +  "," + md5Hash + ",PAYLOAD)");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug(">>> [POST (" + (new Date()) + ")] -> " + endpoint + "/" + resource + " >--------------------------------------------------------------------------------------");
        }
        try {
            HttpClient client = clientFactory.getClient(endpoint);
            HttpPost post = new HttpPost(endpoint + resource);
            httpAuth.addPreemptiveAuth(post);

            post.addHeader("Content-Type", "application/octet-stream");
            post.addHeader("Accept", "application/json");
            post.addHeader("X-Api-Version", VERSION);
            if(strategy != null && strategy.getSendAsHeader()){
                post.addHeader(strategy.getHeaderName(), strategy.getRequestID());
            }

            post.setEntity(new InputStreamEntity(stream, -1L, ContentType.APPLICATION_OCTET_STREAM));
            if( wire.isDebugEnabled() ) {
                wire.debug(post.getRequestLine().toString());
                for( Header header : post.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");

                wire.debug("--> BINARY DATA <--");
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(post);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            logger.debug("HTTP STATUS: " + code);

            String responseHash = null;
            
            for( Header h : response.getAllHeaders() ) {
                if( h.getName().equals("ETag") ) {
                    responseHash = h.getValue(); 
                }
            }
            if( responseHash != null && md5Hash != null && !responseHash.equals(md5Hash) ) {
                throw new CloudException("MD5 hash values do not match, probably data corruption");
            }
            if( code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT && code != HttpStatus.SC_CREATED ) {
                logger.error("Expected ACCEPTED or NO CONTENT for POST request, got " + code);
                String json = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        json = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    throw new CloudException(e);
                }
                JoyentException.ExceptionItems items = JoyentException.parseException(code, json);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                logger.error("[" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                wire.debug("");
                if( code == HttpStatus.SC_ACCEPTED || code == HttpStatus.SC_CREATED ) {
                    String json = null;

                    try {
                        HttpEntity entity = response.getEntity();

                        if( entity != null ) {
                            json = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(json);
                                wire.debug("");
                            }
                        }
                    }
                    catch( IOException e ) {
                        logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if( json != null && !json.trim().equals("") ) {
                        return json;
                    }
                }
                return null;
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + JoyentMethod.class.getName() + ".doPostStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("<<< [POST (" + (new Date()) + ")] -> " + endpoint + "/" + resource + " <--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }
    
    protected @Nullable String putHeaders(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nullable Map<String,String> customHeaders) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + JoyentMethod.class.getName() + ".doPutHeaders(" + endpoint + "," + resource +  "," + customHeaders + ")");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug(">>> [PUT (" + (new Date()) + ")] -> " + endpoint + "/" + resource + " >--------------------------------------------------------------------------------------");
        }
        try {
            HttpClient client = clientFactory.getClient(endpoint);
            HttpPut put = new HttpPut(endpoint + resource);
            
            put.addHeader("Content-Type", "application/json");
            put.addHeader("Accept", "application/json");
            put.addHeader("X-Auth-Token", authToken);
            if( customHeaders != null ) {
                for( Map.Entry<String, String> entry : customHeaders.entrySet() ) {
                    String val = (entry.getValue() == null ? "" : entry.getValue());
                    
                    put.addHeader(entry.getKey(), val);
                }
            }
            if(strategy != null && strategy.getSendAsHeader()){
                put.addHeader(strategy.getHeaderName(), strategy.getRequestID());
            }

            if( wire.isDebugEnabled() ) {
                wire.debug(put.getRequestLine().toString());
                for( Header header : put.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");

                try { wire.debug(EntityUtils.toString(put.getEntity())); }
                catch( IOException ignore ) { }

                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(put);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            logger.debug("HTTP STATUS: " + code);

            if( code != HttpStatus.SC_CREATED && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT ) {
                logger.error("Expected CREATED, ACCEPTED, or NO CONTENT for put request, got " + code);
                String json = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        json = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    throw new CloudException(e);
                }
                JoyentException.ExceptionItems items = JoyentException.parseException(code, json);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                logger.error("[" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                if( code == HttpStatus.SC_ACCEPTED || code == HttpStatus.SC_CREATED ) {
                    String json = null;

                    try {
                        HttpEntity entity = response.getEntity();

                        if( entity != null ) {
                            json = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(json);
                                wire.debug("");
                            }
                        }
                    }
                    catch( IOException e ) {
                        logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if( json != null && !json.trim().equals("") ) {
                        return json;
                    }
                }
                return null;
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + JoyentMethod.class.getName() + ".doPutHeaders()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("<<< [PUT (" + (new Date()) + ")] -> " + endpoint + "/" + resource + " <--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }
    
    protected String putString(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nullable String payload) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + JoyentMethod.class.getName() + ".doPutString(" + endpoint + "," + resource +  ",PAYLOAD)");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug(">>> [PUT (" + (new Date()) + ")] -> " + endpoint + resource + " >--------------------------------------------------------------------------------------");
        }
        try {
            HttpClient client = clientFactory.getClient(endpoint);
            HttpPut put = new HttpPut(endpoint + resource);
            
            put.addHeader("Content-Type", "application/json");
            put.addHeader("Accept", "application/json");
            put.addHeader("X-Auth-Token", authToken);
            if(strategy != null && strategy.getSendAsHeader()){
                put.addHeader(strategy.getHeaderName(), strategy.getRequestID());
            }

            put.setEntity(new StringEntity(payload == null ? "" : payload, APPLICATION_JSON_UTF8));

            if( wire.isDebugEnabled() ) {
                wire.debug(put.getRequestLine().toString());
                for( Header header : put.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");

                try { wire.debug(EntityUtils.toString(put.getEntity())); }
                catch( IOException ignore ) { }

                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(put);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            logger.debug("HTTP STATUS: " + code);

            if( code != HttpStatus.SC_CREATED && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT ) {
                logger.error("Expected CREATED, ACCEPTED, or NO CONTENT for put request, got " + code);
                String json = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        json = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    throw new CloudException(e);
                }
                JoyentException.ExceptionItems items = JoyentException.parseException(code, json);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                logger.error("[" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                if( code == HttpStatus.SC_ACCEPTED || code == HttpStatus.SC_CREATED ) {
                    String json = null;

                    try {
                        HttpEntity entity = response.getEntity();

                        if( entity != null ) {
                            json = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(json);
                                wire.debug("");
                            }
                        }
                    }
                    catch( IOException e ) {
                        logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if( json != null && !json.trim().equals("") ) {
                        return json;
                    }
                }
                return null;
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + JoyentMethod.class.getName() + ".doPutString()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("<<< [PUT (" + (new Date()) + ")] -> " + endpoint + resource + " <--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }

    protected @Nullable String putStream(@Nonnull String authToken, @Nonnull String endpoint, @Nonnull String resource, @Nullable String md5Hash, @Nullable InputStream stream) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + JoyentMethod.class.getName() + ".doPutStream(" + endpoint + "," + resource +  "," + md5Hash + ",PAYLOAD)");
        }
        if( wire.isDebugEnabled() ) {
            wire.debug("");
            wire.debug(">>> [PUT (" + (new Date()) + ")] -> " + endpoint + resource + " >--------------------------------------------------------------------------------------");
        }
        try {
            HttpClient client = clientFactory.getClient(endpoint);
            HttpPut put = new HttpPut(endpoint + resource);
            
            put.addHeader("Content-Type", "application/octet-stream");
            put.addHeader("Accept", "application/json");
            put.addHeader("X-Auth-Token", authToken);
            if( md5Hash != null ) {
                put.addHeader("ETag", md5Hash);
            }
            if(strategy != null && strategy.getSendAsHeader()){
                put.addHeader(strategy.getHeaderName(), strategy.getRequestID());
            }

            put.setEntity(new InputStreamEntity(stream, -1L, ContentType.APPLICATION_OCTET_STREAM));
            if( wire.isDebugEnabled() ) {
                wire.debug(put.getRequestLine().toString());
                for( Header header : put.getAllHeaders() ) {
                    wire.debug(header.getName() + ": " + header.getValue());
                }
                wire.debug("");

                wire.debug("--> BINARY DATA <--");
                wire.debug("");
            }
            HttpResponse response;

            try {
                response = client.execute(put);
                if( wire.isDebugEnabled() ) {
                    wire.debug(response.getStatusLine().toString());
                    for( Header header : response.getAllHeaders() ) {
                        wire.debug(header.getName() + ": " + header.getValue());
                    }
                    wire.debug("");
                }
            }
            catch( IOException e ) {
                logger.error("I/O error from server communications: " + e.getMessage());
                throw new InternalException(e);
            }
            int code = response.getStatusLine().getStatusCode();

            logger.debug("HTTP STATUS: " + code);

            String responseHash = null;

            for( Header h : response.getAllHeaders() ) {
                if( h.getName().equals("ETag") ) {
                    responseHash = h.getValue();
                }
            }
            if( responseHash != null && md5Hash != null && !responseHash.equals(md5Hash) ) {
                throw new CloudException("MD5 hash values do not match, probably data corruption");
            }

            if( code != HttpStatus.SC_CREATED && code != HttpStatus.SC_ACCEPTED && code != HttpStatus.SC_NO_CONTENT ) {
                logger.error("Expected CREATED, ACCEPTED, or NO CONTENT for PUT request, got " + code);
                String json = null;

                try {
                    HttpEntity entity = response.getEntity();

                    if( entity != null ) {
                        json = EntityUtils.toString(entity);
                        if( wire.isDebugEnabled() ) {
                            wire.debug(json);
                            wire.debug("");
                        }
                    }
                }
                catch( IOException e ) {
                    logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                    throw new CloudException(e);
                }
                JoyentException.ExceptionItems items = JoyentException.parseException(code, json);
                
                if( items == null ) {
                    items = new JoyentException.ExceptionItems();
                    items.code = 404;
                    items.type = CloudErrorType.COMMUNICATION;
                    items.message = "itemNotFound";
                    items.details = "No such object: " + resource;
                }
                logger.error("[" +  code + " : " + items.message + "] " + items.details);
                throw new JoyentException(items);
            }
            else {
                if( code == HttpStatus.SC_ACCEPTED ) {
                    String json = null;

                    try {
                        HttpEntity entity = response.getEntity();

                        if( entity != null ) {
                            json = EntityUtils.toString(entity);
                            if( wire.isDebugEnabled() ) {
                                wire.debug(json);
                                wire.debug("");
                            }
                        }
                    }
                    catch( IOException e ) {
                        logger.error("Failed to read response error due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if( json != null && !json.trim().equals("") ) {
                        return json;
                    }
                }
                return null;
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + JoyentMethod.class.getName() + ".doPutStream()");
            }
            if( wire.isDebugEnabled() ) {
                wire.debug("<<< [PUT (" + (new Date()) + ")] -> " + endpoint + resource + " <--------------------------------------------------------------------------------------");
                wire.debug("");
            }
        }
    }

}
