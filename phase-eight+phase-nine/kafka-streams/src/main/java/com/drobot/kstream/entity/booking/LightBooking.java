package com.drobot.kstream.entity.booking;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@SuppressWarnings("unused")
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class LightBooking implements Cloneable {

    public static class JsonField {
        public static final String ID = "id";
        public static final String HOTEL_ID = "hotel_id";
        public static final String SRCH_CI = "srch_ci";
        public static final String SRCH_CO = "srch_co";
        public static final String SRCH_ADULTS_CNT = "srch_adults_cnt";
        public static final String SRCH_CHILDREN_CNT = "srch_children_cnt";
        public static final String USER_ID = "user_id";
    }

    private long id;
    private long hotelId;
    private int userId;
    private String srchCi;
    private String srchCo;
    private int srchAdultsCnt;
    private int srchChildrenCnt;

    protected LightBooking() {
    }

    @Override
    public LightBooking clone() throws CloneNotSupportedException {
        LightBooking cloned = (LightBooking) super.clone();
        cloned.id = id;
        cloned.hotelId = hotelId;
        cloned.userId = userId;
        cloned.srchCi = srchCi;
        cloned.srchCo = srchCo;
        cloned.srchAdultsCnt = srchAdultsCnt;
        cloned.srchChildrenCnt = srchChildrenCnt;
        return cloned;
    }

    public long getId() {
        return id;
    }

    public long getHotelId() {
        return hotelId;
    }

    public int getUserId() {
        return userId;
    }

    public String getSrchCi() {
        return srchCi;
    }


    public String getSrchCo() {
        return srchCo;
    }

    public int getSrchAdultsCnt() {
        return srchAdultsCnt;
    }

    public int getSrchChildrenCnt() {
        return srchChildrenCnt;
    }

    private void setId(long id) {
        this.id = id;
    }

    private void setHotelId(long hotelId) {
        this.hotelId = hotelId;
    }

    private void setUserId(int userId) {
        this.userId = userId;
    }

    private void setSrchCi(String srchCi) {
        this.srchCi = srchCi;
    }

    private void setSrchCo(String srchCo) {
        this.srchCo = srchCo;
    }

    private void setSrchAdultsCnt(int srchAdultsCnt) {
        this.srchAdultsCnt = srchAdultsCnt;
    }

    private void setSrchChildrenCnt(int srchChildrenCnt) {
        this.srchChildrenCnt = srchChildrenCnt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LightBooking that = (LightBooking) o;
        if (id != that.id) {
            return false;
        }
        if (hotelId != that.hotelId) {
            return false;
        }
        if (userId != that.userId) {
            return false;
        }
        if (srchAdultsCnt != that.srchAdultsCnt) {
            return false;
        }
        if (srchChildrenCnt != that.srchChildrenCnt) {
            return false;
        }
        if (srchCi != null ? !srchCi.equals(that.srchCi) : that.srchCi != null) {
            return false;
        }
        return srchCo != null ? srchCo.equals(that.srchCo) : that.srchCo == null;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (int) (hotelId ^ (hotelId >>> 32));
        result = 31 * result + userId;
        result = 31 * result + (srchCi != null ? srchCi.hashCode() : 0);
        result = 31 * result + (srchCo != null ? srchCo.hashCode() : 0);
        result = 31 * result + srchAdultsCnt;
        result = 31 * result + srchChildrenCnt;
        return result;
    }

    @Override
    public String toString() {
        return "LightBooking{" +
                "id=" + id +
                ", hotelId=" + hotelId +
                ", userId=" + userId +
                ", srchCi='" + srchCi + '\'' +
                ", srchCo='" + srchCo + '\'' +
                ", srchAdultsCnt=" + srchAdultsCnt +
                ", srchChildrenCnt=" + srchChildrenCnt +
                '}';
    }
}
