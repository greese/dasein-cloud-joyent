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
