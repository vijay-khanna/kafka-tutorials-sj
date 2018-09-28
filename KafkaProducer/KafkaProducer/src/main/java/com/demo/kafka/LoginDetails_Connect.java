package com.demo.kafka;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LoginDetails_Connect implements Serializable{
    
    /**
     * {"username":"nirav","product":"demo","country":"India","tz":"GMT+05:30","browser":"Chrome","browserVersion":"67","os":"Windows 7","osVersion":"7","deviceType":"desktop","@timestamp":"2018-09-17 15:01:03"}
     */
    private static final long serialVersionUID = 8757809179891063548L;
    private String username;
    private String product;
    private String browser;
    private String browserVersion;
    private String os;
    private String osVersion;
    private String deviceType;
    private String tz;
    private String country;
    @JsonProperty("@timestamp")
    private String timestamp;
    
    public LoginDetails_Connect(){
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

    public String getUsername() {
        return username;
    }
    public String getProduct() {
        return product;
    }

    public String getCountry() {
        return country;
    }

    
}
