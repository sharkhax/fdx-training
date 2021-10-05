package com.drobot.kstream.entity.booking;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@SuppressWarnings("unused")
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class ExtendedBooking extends LightBooking implements Cloneable {

    public static class JsonField extends LightBooking.JsonField {
        public static final String DATE_TIME = "date_time";
        public static final String SITE_NAME = "site_name";
        public static final String POSA_CONTINENT = "posa_continent";
        public static final String USER_LOCATION_COUNTRY = "user_location_country";
        public static final String USER_LOCATION_REGION = "user_location_region";
        public static final String USER_LOCATION_CITY = "user_location_city";
        public static final String ORIG_DESTINATION_DISTANCE = "orig_destination_distance";
        public static final String IS_MOBILE = "is_mobile";
        public static final String IS_PACKAGE = "is_package";
        public static final String CHANNEL = "channel";
        public static final String SRCH_RM_CNT = "srch_rm_cnt";
        public static final String SRCH_DESTINATION_ID = "srch_destination_id";
        public static final String SRCH_DESTINATION_TYPE_ID = "srch_destination_type_id";
    }

    private String dateTime;
    private int siteName;
    private int posaContinent;
    private int userLocationCity;
    private int userLocationRegion;
    private int userLocationCountry;
    private double origDestinationDistance;
    private int isMobile;
    private int isPackage;
    private int channel;
    private int srchRmCnt;
    private int srchDestinationId;
    private int srchDestinationTypeId;

    protected ExtendedBooking() {
    }

    @Override
    public ExtendedBooking clone() throws CloneNotSupportedException {
        ExtendedBooking cloned = (ExtendedBooking) super.clone();
        cloned.dateTime = dateTime;
        cloned.siteName = siteName;
        cloned.posaContinent = posaContinent;
        cloned.userLocationCity = userLocationCity;
        cloned.userLocationRegion = userLocationRegion;
        cloned.userLocationCountry = userLocationCountry;
        cloned.origDestinationDistance = origDestinationDistance;
        cloned.isMobile = isMobile;
        cloned.isPackage = isPackage;
        cloned.channel = channel;
        cloned.srchRmCnt = srchRmCnt;
        cloned.srchDestinationId = srchDestinationId;
        cloned.srchDestinationTypeId = srchDestinationTypeId;
        return cloned;
    }

    public String getDateTime() {
        return dateTime;
    }

    public int getSiteName() {
        return siteName;
    }

    public int getPosaContinent() {
        return posaContinent;
    }

    public int getUserLocationCity() {
        return userLocationCity;
    }

    public int getUserLocationRegion() {
        return userLocationRegion;
    }

    public int getUserLocationCountry() {
        return userLocationCountry;
    }

    public double getOrigDestinationDistance() {
        return origDestinationDistance;
    }

    public int getIsMobile() {
        return isMobile;
    }

    public int getIsPackage() {
        return isPackage;
    }

    public int getChannel() {
        return channel;
    }

    public int getSrchRmCnt() {
        return srchRmCnt;
    }

    public int getSrchDestinationId() {
        return srchDestinationId;
    }

    public int getSrchDestinationTypeId() {
        return srchDestinationTypeId;
    }

    private void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    private void setSiteName(int siteName) {
        this.siteName = siteName;
    }

    private void setPosaContinent(int posaContinent) {
        this.posaContinent = posaContinent;
    }

    private void setUserLocationCity(int userLocationCity) {
        this.userLocationCity = userLocationCity;
    }

    private void setUserLocationRegion(int userLocationRegion) {
        this.userLocationRegion = userLocationRegion;
    }

    private void setUserLocationCountry(int userLocationCountry) {
        this.userLocationCountry = userLocationCountry;
    }

    private void setOrigDestinationDistance(double origDestinationDistance) {
        this.origDestinationDistance = origDestinationDistance;
    }

    private void setIsMobile(int isMobile) {
        this.isMobile = isMobile;
    }

    private void setIsPackage(int isPackage) {
        this.isPackage = isPackage;
    }

    private void setChannel(int channel) {
        this.channel = channel;
    }

    private void setSrchRmCnt(int srchRmCnt) {
        this.srchRmCnt = srchRmCnt;
    }

    private void setSrchDestinationId(int srchDestinationId) {
        this.srchDestinationId = srchDestinationId;
    }

    private void setSrchDestinationTypeId(int srchDestinationTypeId) {
        this.srchDestinationTypeId = srchDestinationTypeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ExtendedBooking that = (ExtendedBooking) o;
        if (siteName != that.siteName) {
            return false;
        }
        if (posaContinent != that.posaContinent) {
            return false;
        }
        if (userLocationCity != that.userLocationCity) {
            return false;
        }
        if (userLocationRegion != that.userLocationRegion) {
            return false;
        }
        if (userLocationCountry != that.userLocationCountry) {
            return false;
        }
        if (Double.compare(that.origDestinationDistance, origDestinationDistance) != 0) {
            return false;
        }
        if (isMobile != that.isMobile) {
            return false;
        }
        if (isPackage != that.isPackage) {
            return false;
        }
        if (channel != that.channel) {
            return false;
        }
        if (srchRmCnt != that.srchRmCnt) {
            return false;
        }
        if (srchDestinationId != that.srchDestinationId) {
            return false;
        }
        if (srchDestinationTypeId != that.srchDestinationTypeId) {
            return false;
        }
        return dateTime != null ? dateTime.equals(that.dateTime) : that.dateTime == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        result = 31 * result + (dateTime != null ? dateTime.hashCode() : 0);
        result = 31 * result + siteName;
        result = 31 * result + posaContinent;
        result = 31 * result + userLocationCity;
        result = 31 * result + userLocationRegion;
        result = 31 * result + userLocationCountry;
        temp = Double.doubleToLongBits(origDestinationDistance);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + isMobile;
        result = 31 * result + isPackage;
        result = 31 * result + channel;
        result = 31 * result + srchRmCnt;
        result = 31 * result + srchDestinationId;
        result = 31 * result + srchDestinationTypeId;
        return result;
    }

    @Override
    public String toString() {
        return "ExtendedBooking{" +
                "dateTime='" + dateTime + '\'' +
                ", siteName=" + siteName +
                ", posaContinent=" + posaContinent +
                ", userLocationCity=" + userLocationCity +
                ", userLocationRegion=" + userLocationRegion +
                ", userLocationCountry=" + userLocationCountry +
                ", origDestinationDistance=" + origDestinationDistance +
                ", isMobile=" + isMobile +
                ", isPackage=" + isPackage +
                ", channel=" + channel +
                ", srchRmCnt=" + srchRmCnt +
                ", srchDestinationId=" + srchDestinationId +
                ", srchDestinationTypeId=" + srchDestinationTypeId +
                "} " + super.toString();
    }
}
