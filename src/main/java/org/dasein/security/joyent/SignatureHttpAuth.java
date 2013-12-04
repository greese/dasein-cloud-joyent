package org.dasein.security.joyent;


import org.apache.http.HttpRequest;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;
import org.bouncycastle.util.encoders.Base64;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.joyent.storage.Manta;

import javax.annotation.Nonnull;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.security.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

public class SignatureHttpAuth implements JoyentHttpAuth {
    private static final DateFormat RFC1123_DATE_FORMAT = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z");
    private static final String AUTH_HEADER = "Signature keyId=\"/%s/keys/%s\",algorithm=\"rsa-sha256\",signature=\"%s\"";
    private static final String AUTH_SIGN = "date: %s";
    private static final String SIGN_ALGORITHM = "SHA256WithRSAEncryption";

    private ProviderContext providerContext;

    public SignatureHttpAuth(ProviderContext providerContext) {
        this.providerContext = providerContext;
    }

    @Override
    public void addPreemptiveAuth(@Nonnull HttpRequest request) throws CloudException, InternalException {
        if( providerContext == null ) {
            throw new CloudException("No context was defined for this request");
        }
        Properties customProperties = providerContext.getCustomProperties();
        Date date = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime();
        String now = RFC1123_DATE_FORMAT.format(date);
        request.setHeader("Date", now);
        try {
            Signature signature = Signature.getInstance(SIGN_ALGORITHM);
            signature.initSign(getKeyPair(customProperties.getProperty(Manta.CUSTOM_PROP_KEY_PATH)).getPrivate());
            String signingString = String.format(AUTH_SIGN, now);
            signature.update(signingString.getBytes("UTF-8"));
            byte[] signedDate = signature.sign();
            byte[] encodedSignedDate = Base64.encode(signedDate);

            request.addHeader("Authorization", String.format(
                    AUTH_HEADER,
                    providerContext.getAccountNumber(),
                    customProperties.getProperty(Manta.CUSTOM_PROP_KEY_FINGERPRINT),
                    new String(encodedSignedDate)));
        } catch (NoSuchAlgorithmException e) {
            throw new InternalException(e);
        } catch (UnsupportedEncodingException e) {
            throw new InternalException(e);
        } catch (SignatureException e) {
            throw new InternalException(e);
        } catch (InvalidKeyException e) {
            throw new InternalException(e);
        } catch (IOException e) {
            throw new InternalException(e);
        }
    }

    private KeyPair getKeyPair(String keyPath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(keyPath));
        Security.addProvider(new BouncyCastleProvider());
        PEMReader pemReader = new PEMReader(reader);
        try {
             return (KeyPair) pemReader.readObject();
        } finally {
            reader.close();
            pemReader.close();
        }
    }
}
