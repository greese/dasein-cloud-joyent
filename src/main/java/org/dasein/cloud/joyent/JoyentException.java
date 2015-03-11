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

import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.json.JSONException;
import org.json.JSONObject;

public class JoyentException extends CloudException {
    private static final long serialVersionUID = 1144131377092530264L;


    static public class ExceptionItems {
        public CloudErrorType type;
        public int code;
        public String message;
        public String details;
    }
    
    static public ExceptionItems parseException(int code, String json) {
        ExceptionItems items = new ExceptionItems();
        
        items.code = code;
        items.type = CloudErrorType.GENERAL;
        items.message = "unknown";
        items.details = "The cloud provided an error code without explanation";
        
        if( json != null ) {
            try {
                JSONObject ob = new JSONObject(json);
                
                if( ob.has("message") ) {
                    items.details = ob.getString("message");
                }
                else {
                    items.details = "[" + code + "] " + items.message;
                }
                if( ob.has("code") ) {
                    items.message = ob.getString("code");
                    if( items.message == null ) {
                        items.message = "unknown";
                    }
                    else {
                        items.message = items.message.trim();
                    }
                }
                else {
                    items.message = "unknown";
                }
                String t = items.message.toLowerCase().trim();
                
                if( t.equals("notauthorized") || t.equals("invalidcredentials") ) {
                    items.type = CloudErrorType.AUTHENTICATION;
                }
                else if( t.equals("requestthrottled") ) {
                    items.type = CloudErrorType.CAPACITY;
                }
                else if( t.equals("requesttoolarge") || t.equals("badrequest") || t.equals("invalidargument") || t.equals("invalidheader") || t.equals("invalidversion") || t.equals("missingparameter") ) {
                    items.type = CloudErrorType.COMMUNICATION;
                }
                else if( t.equals("resourcenotfound") ) {
                    return null;
                }
            }
            catch( JSONException e ) {
                SmartDataCenter.getLogger(JoyentException.class, "std").warn("parseException(): Invalid JSON in cloud response: " + json);
                
                items.details = json;
            }
        }
        return items;
    }
    
    public JoyentException(ExceptionItems items) {
        super(items.type, items.code, items.message, items.details);
    }
}
