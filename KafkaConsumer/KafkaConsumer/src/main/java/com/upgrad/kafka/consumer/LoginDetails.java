package com.upgrad.kafka.consumer;

import java.io.Serializable;

public class LoginDetails implements Serializable{
    
    /**
     * 
     */
    private static final long serialVersionUID = 8757809179891063548L;
    private String username;
    private String product;
    private String userAgent;
    private String language;
    private String ipAddress;
    private String loginTime;
    private String browser;
    private String browserVersion;
    private String os;
    private String osVersion;
    private String deviceType;
    private String tz;
    private String country;
    
    public LoginDetails(){
    }
    
    public LoginDetails(String username, String product) {
        super();
        this.username = username;
        this.product = product;
    }
    
    public LoginDetails(String username, String product, String userAgent, String language, String ipAddress) {
        super();
        this.username = username;
        this.product = product;
        this.userAgent = userAgent;
        this.language = language;
        this.ipAddress = ipAddress;
    }
    
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getBrowser() {
        return browser;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public String getOs() {
        return os;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public String getTz() {
        return tz;
    }

    public String getLoginTime() {
        return loginTime;
    }

    public String getUserAgent() {
        return userAgent;
    }
    public String getLanguage() {
        return language;
    }
    public String getIpAddress() {
        return ipAddress;
    }
    public String getUsername() {
        return username;
    }
    public String getProduct() {
        return product;
    }

    public String getCountry() {
        return country;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((browser == null) ? 0 : browser.hashCode());
        result = prime * result + ((browserVersion == null) ? 0 : browserVersion.hashCode());
        result = prime * result + ((country == null) ? 0 : country.hashCode());
        result = prime * result + ((deviceType == null) ? 0 : deviceType.hashCode());
        result = prime * result + ((ipAddress == null) ? 0 : ipAddress.hashCode());
        result = prime * result + ((language == null) ? 0 : language.hashCode());
        result = prime * result + ((loginTime == null) ? 0 : loginTime.hashCode());
        result = prime * result + ((os == null) ? 0 : os.hashCode());
        result = prime * result + ((osVersion == null) ? 0 : osVersion.hashCode());
        result = prime * result + ((product == null) ? 0 : product.hashCode());
        result = prime * result + ((tz == null) ? 0 : tz.hashCode());
        result = prime * result + ((userAgent == null) ? 0 : userAgent.hashCode());
        result = prime * result + ((username == null) ? 0 : username.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LoginDetails other = (LoginDetails) obj;
        if (browser == null) {
            if (other.browser != null)
                return false;
        } else if (!browser.equals(other.browser))
            return false;
        if (browserVersion == null) {
            if (other.browserVersion != null)
                return false;
        } else if (!browserVersion.equals(other.browserVersion))
            return false;
        if (country == null) {
            if (other.country != null)
                return false;
        } else if (!country.equals(other.country))
            return false;
        if (deviceType == null) {
            if (other.deviceType != null)
                return false;
        } else if (!deviceType.equals(other.deviceType))
            return false;
        if (ipAddress == null) {
            if (other.ipAddress != null)
                return false;
        } else if (!ipAddress.equals(other.ipAddress))
            return false;
        if (language == null) {
            if (other.language != null)
                return false;
        } else if (!language.equals(other.language))
            return false;
        if (loginTime == null) {
            if (other.loginTime != null)
                return false;
        } else if (!loginTime.equals(other.loginTime))
            return false;
        if (os == null) {
            if (other.os != null)
                return false;
        } else if (!os.equals(other.os))
            return false;
        if (osVersion == null) {
            if (other.osVersion != null)
                return false;
        } else if (!osVersion.equals(other.osVersion))
            return false;
        if (product == null) {
            if (other.product != null)
                return false;
        } else if (!product.equals(other.product))
            return false;
        if (tz == null) {
            if (other.tz != null)
                return false;
        } else if (!tz.equals(other.tz))
            return false;
        if (userAgent == null) {
            if (other.userAgent != null)
                return false;
        } else if (!userAgent.equals(other.userAgent))
            return false;
        if (username == null) {
            if (other.username != null)
                return false;
        } else if (!username.equals(other.username))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "LoginDetails [username=" + username + ", product=" + product + ", userAgent=" + userAgent + ", language=" + language
                + ", ipAddress=" + ipAddress + ", loginTime=" + loginTime + ", browser=" + browser + ", browserVersion=" + browserVersion
                + ", os=" + os + ", osVersion=" + osVersion + ", deviceType=" + deviceType + ", tz=" + tz + ", country=" + country + "]";
    }
    
}
