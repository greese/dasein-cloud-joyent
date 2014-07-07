package org.dasein.cloud.joyent;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.dc.DataCenterCapabilities;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * User: daniellemayne
 * Date: 04/07/2014
 * Time: 16:30
 */
public class JoyentDataCenterCapabilities extends AbstractCapabilities<SmartDataCenter> implements DataCenterCapabilities {
    public JoyentDataCenterCapabilities(@Nonnull SmartDataCenter provider) {
        super(provider);
    }
    @Override
    public String getProviderTermForDataCenter(Locale locale) {
        return "dc zone";
    }

    @Override
    public String getProviderTermForRegion(Locale locale) {
        return "datacenter";
    }

    @Override
    public boolean supportsResourcePools() {
        return false;
    }
}
