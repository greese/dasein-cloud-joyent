package org.dasein.security.joyent;

import org.apache.http.client.HttpClient;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;

import javax.annotation.Nonnull;

public interface JoyentClientFactory {

    @Nonnull HttpClient getClient(String endpoint) throws CloudException, InternalException;

}
