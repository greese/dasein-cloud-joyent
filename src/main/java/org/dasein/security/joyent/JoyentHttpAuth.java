package org.dasein.security.joyent;

import org.apache.http.HttpRequest;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;

import javax.annotation.Nonnull;

public interface JoyentHttpAuth {

    void addPreemptiveAuth(@Nonnull HttpRequest request) throws CloudException, InternalException;

}
