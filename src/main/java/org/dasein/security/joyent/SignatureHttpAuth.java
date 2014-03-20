package org.dasein.security.joyent;


import org.apache.http.HttpRequest;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMReader;
import org.bouncycastle.openssl.PasswordFinder;
import org.bouncycastle.util.encoders.Base64;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.ContextRequirements;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.joyent.SmartDataCenter;
import org.dasein.cloud.joyent.storage.Manta;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.security.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class SignatureHttpAuth implements JoyentHttpAuth {
    private static final DateFormat RFC1123_DATE_FORMAT = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z");
    private static final String AUTH_HEADER = "Signature keyId=\"/%s/keys/%s\",algorithm=\"rsa-sha256\",signature=\"%s\"";
    private static final String AUTH_SIGN = "date: %s";
    private static final String SIGN_ALGORITHM = "SHA256WithRSAEncryption";

    private SmartDataCenter provider;

    public SignatureHttpAuth(SmartDataCenter provider) {
        this.provider = provider;
    }

    @Override
    public void addPreemptiveAuth(@Nonnull HttpRequest request) throws CloudException, InternalException {
        if( provider.getContext() == null ) {
            throw new CloudException("No context was defined for this request");
        }
        Date date = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime();
        String now = RFC1123_DATE_FORMAT.format(date);
        request.setHeader("Date", now);
        try {
            Security.addProvider(new BouncyCastleProvider());
            Signature signature = Signature.getInstance(SIGN_ALGORITHM);

            List<ContextRequirements.Field> fields = provider.getContextRequirements().getConfigurableValues();
            String keyName = "";
            String privateKey = "";
            String keyPassword = "";
            for(ContextRequirements.Field f : fields ) {
                if(f.type.equals(ContextRequirements.FieldType.KEYPAIR)){
                    byte[][] keyPair = (byte[][])provider.getContext().getConfigurationValue(f);
                    keyName = new String(keyPair[0], "utf-8");
                    privateKey = new String(keyPair[1], "utf-8");
                }
                else if(f.type.equals(ContextRequirements.FieldType.PASSWORD)){
                    keyPassword = new String((byte[])provider.getContext().getConfigurationValue(f), "utf-8");
                }
            }

            signature.initSign(getKeyPair(privateKey, keyPassword.toCharArray()).getPrivate());
            String signingString = String.format(AUTH_SIGN, now);
            signature.update(signingString.getBytes("UTF-8"));
            byte[] signedDate = signature.sign();
            byte[] encodedSignedDate = Base64.encode(signedDate);

            request.addHeader("Authorization", String.format(AUTH_HEADER, provider.getContext().getAccountNumber(), keyName, new String(encodedSignedDate)));

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

    private KeyPair getKeyPair(String privateKeyContent, @Nullable final char[] password) throws IOException {
        InputStream is = new ByteArrayInputStream(privateKeyContent.getBytes());
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        PEMReader pemReader = null;
        if(password != null){
            pemReader = new PEMReader(reader, new PasswordFinder(){
                @Override public char[] getPassword(){
                    return password;
                }
            });
        }
        else{
            pemReader = new PEMReader(reader);
        }
        try {
             return (KeyPair) pemReader.readObject();
        } finally {
            reader.close();
            pemReader.close();
        }
    }
}
