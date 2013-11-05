package org.dasein.security.joyent;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;

import javax.annotation.Nonnull;

public class DefaultClientFactory implements JoyentClientFactory {

    @Override
    public @Nonnull HttpClient getClient(String endpoint) throws CloudException, InternalException {
        return new DefaultHttpClient();
    }
}
