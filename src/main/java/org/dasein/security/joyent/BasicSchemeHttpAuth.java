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

package org.dasein.security.joyent;

import org.apache.http.HttpRequest;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.auth.BasicScheme;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.joyent.SmartDataCenter;

import javax.annotation.Nonnull;
import java.io.UnsupportedEncodingException;

public class BasicSchemeHttpAuth implements JoyentHttpAuth {

    private ProviderContext providerContext;

    public BasicSchemeHttpAuth(ProviderContext providerContext) {
        this.providerContext = providerContext;
    }

    public void addPreemptiveAuth(@Nonnull HttpRequest request) throws CloudException, InternalException {
        if( providerContext == null ) {
            throw new CloudException("No context was defined for this request");
        }
        try {
            String username = new String(providerContext.getAccessPublic(), "utf-8");
            String password = new String(providerContext.getAccessPrivate(), "utf-8");
            UsernamePasswordCredentials creds = new UsernamePasswordCredentials(username, password);
            request.addHeader(new BasicScheme().authenticate(creds, request));
        } catch (UnsupportedEncodingException e) {
            throw new InternalException(e);
        } catch (AuthenticationException e) {
            throw new InternalException(e);
        }
    }
}
