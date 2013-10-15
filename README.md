dasein-cloud-joyent
===================

The Dasein Cloud Joyent submodule to the [Dasein Cloud](https://github.com/greese/dasein-cloud) project provides
an implementation of the Dasein Cloud API for the Joyent Cloud and the SDC platform.

* [Get started with Dasein Cloud](https://github.com/greese/dasein-cloud)
* [Get started with Dasein Cloud + Joyent](https://github.com/greese/dasein-cloud-joyent/wiki)

Configuration
-------------------

To connect to Joynet Manta Sevice you can use ProviderLoader class.

### Provider loader

This class creates ProviderContext object with all necessary data. You have to specify the following system properties to
pass data into ProviderContext object.

Manta configuration properties:

    DSN_PROVIDER_CLASS=org.dasein.cloud.joyent.SmartDataCenter
    DSN_CUSTOM_STORAGE_URL=<MANTA_URL>
    DSN_ACCOUNT=<MANTA_USER>
    DSN_CUSTOM_KEY_PATH=<RSA_PUB_KEY_PATH>
    DSN_CUSTOM_KEY_FINGERPRINT=<MANTA_KEY_ID>

Dasein properties (see Dasein Cloud Joyent [configuration](https://github.com/greese/dasein-cloud-joyent/wiki/Configuration)):

    DSN_API_SHARED=<Redundant value required by DSN. Must not be null>
    DSN_API_SECRET=<Redundant value required by DSN. Must not be null>
    DSN_ENDPOINT=<DASEIN_ROUTE_URL> (alias for "endpoint" provider context value)
    DSN_REGION=<DASEIN_REGION> (alias for "regionId")
    DSN_CLOUD_NAME=<DASEIN_CLOUD_NAME> (alias for "cloudName")
    DSN_CLOUD_PROVIDER=<DASEIN_CLOUD_PROVIDER> (alias for "providerName")

All properties are REQUIRED by tests. Without these properties you should skip maven tests.

Installation
-------------------

### Maven

You need [maven](https://maven.apache.org/) to build this library. After you installed maven run set properties defined
in [Configuration](#configuration) or add '-DskipTests=true'. Then run:

    $ mvn clean install

Then you can use dasein-cloud-joyent/target/dasein-cloud-joyent-{version}.jar as an implementation of Dasein Cloud.

Usage
-------------------

Here is some basic usage of Dasein Cloud Joyent submodule:

### Add maven dependency

    <dependencies>
      <!-- API -->
      <dependency>
        <groupId>org.dasein</groupId>
        <artifactId>dasein-cloud-core</artifactId>
        <version>2013.07.0</version>
        <scope>compile</scope>
        <optional>false</optional>
      </dependency>
      <!-- implementation -->
      <dependency>
        <groupId>org.dasein</groupId>
        <artifactId>dasein-cloud-joyent</artifactId>
        <version>2013.07.1</version>
        <scope>runtime</scope>
        <optional>false</optional>
      </dependency>
    </dependencies>

### Set variables

You can set variables inside maven using maven-surfire-plugin:

    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-surefire-plugin</artifactId>
      <version>2.6</version>
      <configuration>
          <systemPropertyVariables>
              <DSN_PROVIDER_CLASS>org.dasein.cloud.joyent.SmartDataCenter</DSN_PROVIDER_CLASS>
              <DSN_ENDPOINT>https://us-west-1.api.joyentcloud.com</DSN_ENDPOINT>
              <DSN_REGION>us-west</DSN_REGION>
              <DSN_API_SHARED>""</DSN_API_SHARED>
              <DSN_API_SECRET>""</DSN_API_SECRET>
              <DSN_ACCOUNT>altoros2</DSN_ACCOUNT>
              <DSN_CLOUD_NAME>Joyent Cloud</DSN_CLOUD_NAME>
              <DSN_CLOUD_PROVIDER>Joyent</DSN_CLOUD_PROVIDER>
              <DSN_CUSTOM_STORAGE_URL>https://us-east.manta.joyent.com</DSN_CUSTOM_STORAGE_URL>
              <DSN_CUSTOM_KEY_PATH>src/test/resources/data/id_rsa</DSN_CUSTOM_KEY_PATH>
              <DSN_CUSTOM_KEY_FINGERPRINT>04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df</DSN_CUSTOM_KEY_FINGERPRINT>
              <DSN_CLOUD_PROVIDER>Joyent</DSN_CLOUD_PROVIDER>
          </systemPropertyVariables>
      </configuration>
    </plugin>

Or you can set system variables in Java using this snippet:

    Properties props = new Properties();
    InputStream inputStream = getClassLoader().getResourceAsStream("dsn.properties");
    props.load(inputStream);
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
        System.setProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
    }

dsn.properties:

    DSN_PROVIDER_CLASS=org.dasein.cloud.joyent.SmartDataCenter
    DSN_ENDPOINT=https://us-west-1.api.joyentcloud.com
    DSN_REGION=us-west
    DSN_ACCOUNT=altoros2
    DSN_CLOUD_NAME=Joyent Cloud
    DSN_CLOUD_PROVIDER=Joyent
    DSN_CUSTOM_STORAGE_URL=https://us-east.manta.joyent.com
    DSN_CUSTOM_KEY_PATH=src/test/resources/data/id_rsa
    DSN_CUSTOM_KEY_FINGERPRINT=04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df
    DSN_API_SHARED=
    DSN_API_SECRET=

### Upload sample

    package org.example.dasein.storage;

    import org.dasein.cloud.CloudException;
    import org.dasein.cloud.InternalException;
    import org.dasein.cloud.examples.ProviderLoader;
    import org.dasein.cloud.storage.Blob;
    import org.dasein.cloud.storage.BlobStoreSupport;

    import java.io.File;
    import java.io.UnsupportedEncodingException;


    public class SimpleClientTest {
        private static final String FILE_PATH = "/path/to/you/file/Master-Yoda.jpg";
        private static final String OBJECT_NAME = "Master-Yoda.jpg";
        private static final String OBJECT_PATH = "/altoros2/stor/1/";

        public static void main(String[] args) throws ClassNotFoundException, InstantiationException,
                UnsupportedEncodingException, IllegalAccessException, CloudException, InternalException {
            BlobStoreSupport storage = new ProviderLoader().getConfiguredProvider().getStorageServices().getOnlineStorageSupport();
            Blob uploaded = storage.upload(new File(FILE_PATH), OBJECT_PATH, OBJECT_NAME);
            System.out.print(uploaded);
        }
    }

For more examples, check the included unit tests.
