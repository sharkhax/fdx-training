package com.drobot.test;

import com.drobot.kstream.processor.ExtendedToLightBookingsMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ExtendedToLightBookingsMapperTest {

    @Test
    public void fieldsRemovalTest() {
        ValueMapper<String, String> mapper = new ExtendedToLightBookingsMapper();
        List<String> values = initValues();
        List<String> expected = expectedValues();
        List<String> actual = values.stream().map(mapper::apply).collect(Collectors.toList());
        Assertions.assertEquals(expected, actual);
    }

    private List<String> initValues() {
        String value1 = "{\"id\": 104, \"date_time\": \"2015-07-07 17:05:10\", \"site_name\": 11, \"posa_continent\": 3, \"user_location_country\": 205, \"user_location_region\": 312, \"user_location_city\": 22859, \"orig_destination_distance\": 60.3816, \"user_id\": 414, \"is_mobile\": 0, \"is_package\": 0, \"channel\": 10, \"srch_ci\": \"2017-08-03\", \"srch_co\": \"2017-08-04\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 2, \"srch_rm_cnt\": 1, \"srch_destination_id\": 53976, \"srch_destination_type_id\": 4, \"hotel_id\": 2310692405248}";
        String value2 = "{\"id\": 260, \"date_time\": \"2015-07-07 00:32:58\", \"site_name\": 2, \"posa_continent\": 3, \"user_location_country\": 3, \"user_location_region\": 50, \"user_location_city\": 5224, \"orig_destination_distance\": null, \"user_id\": 951, \"is_mobile\": 0, \"is_package\": 0, \"channel\": 10, \"srch_ci\": \"2016-10-20\", \"srch_co\": \"2016-10-21\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 0, \"srch_rm_cnt\": 2, \"srch_destination_id\": 41746, \"srch_destination_type_id\": 4, \"hotel_id\": 1546188226565}";
        String value3 = "{\"id\": 368, \"date_time\": \"2015-05-16 11:23:14\", \"site_name\": 2, \"posa_continent\": 3, \"user_location_country\": 66, \"user_location_region\": 174, \"user_location_city\": 46432, \"orig_destination_distance\": 133.5031, \"user_id\": 1277, \"is_mobile\": 1, \"is_package\": 0, \"channel\": 0, \"srch_ci\": \"2017-08-30\", \"srch_co\": \"2017-08-31\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 1, \"srch_rm_cnt\": 1, \"srch_destination_id\": 8834, \"srch_destination_type_id\": 1, \"hotel_id\": 3255585210372}";
        String value4 = "{\"id\": 409, \"date_time\": \"2015-05-23 16:00:40\", \"site_name\": 24, \"posa_continent\": 2, \"user_location_country\": 3, \"user_location_region\": 50, \"user_location_city\": 5224, \"orig_destination_distance\": null, \"user_id\": 1383, \"is_mobile\": 1, \"is_package\": 0, \"channel\": 4, \"srch_ci\": \"2017-08-29\", \"srch_co\": \"2017-09-01\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 0, \"srch_rm_cnt\": 1, \"srch_destination_id\": 51923, \"srch_destination_type_id\": 1, \"hotel_id\": 1314259992578}";
        String value5 = "{\"id\": 526, \"date_time\": \"2015-09-16 09:29:22\", \"site_name\": 13, \"posa_continent\": 1, \"user_location_country\": 46, \"user_location_region\": 172, \"user_location_city\": 40514, \"orig_destination_distance\": 5036.1736, \"user_id\": 1640, \"is_mobile\": 0, \"is_package\": 0, \"channel\": 5, \"srch_ci\": \"2016-10-18\", \"srch_co\": \"2016-10-20\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 0, \"srch_rm_cnt\": 1, \"srch_destination_id\": 14984, \"srch_destination_type_id\": 1, \"hotel_id\": 1623497637889}";
        String value6 = "{\"id\": 740, \"date_time\": \"2015-05-13 02:58:48\", \"site_name\": 24, \"posa_continent\": 2, \"user_location_country\": 3, \"user_location_region\": 50, \"user_location_city\": 5224, \"orig_destination_distance\": null, \"user_id\": 2458, \"is_mobile\": 0, \"is_package\": 0, \"channel\": 3, \"srch_ci\": \"2016-10-26\", \"srch_co\": \"2016-10-30\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 0, \"srch_rm_cnt\": 1, \"srch_destination_id\": 8279, \"srch_destination_type_id\": 1, \"hotel_id\": 429496729602}";
        String value7 = "{\"id\": 762, \"date_time\": \"2015-11-28 13:59:19\", \"site_name\": 24, \"posa_continent\": 2, \"user_location_country\": 3, \"user_location_region\": 52, \"user_location_city\": 3015, \"orig_destination_distance\": null, \"user_id\": 2516, \"is_mobile\": 0, \"is_package\": 0, \"channel\": 6, \"srch_ci\": \"2016-10-03\", \"srch_co\": \"2016-10-05\", \"srch_adults_cnt\": 1, \"srch_children_cnt\": 0, \"srch_rm_cnt\": 1, \"srch_destination_id\": 8281, \"srch_destination_type_id\": 1, \"hotel_id\": 1632087572484}";
        String value8 = "{\"id\": 944, \"date_time\": \"2015-05-16 06:04:14\", \"site_name\": 2, \"posa_continent\": 3, \"user_location_country\": 66, \"user_location_region\": 318, \"user_location_city\": 23659, \"orig_destination_distance\": 48.2187, \"user_id\": 3070, \"is_mobile\": 0, \"is_package\": 0, \"channel\": 10, \"srch_ci\": \"2017-09-17\", \"srch_co\": \"2017-09-18\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 0, \"srch_rm_cnt\": 1, \"srch_destination_id\": 41550, \"srch_destination_type_id\": 6, \"hotel_id\": 2482491097091}";
        String value9 = "{\"id\": 1151, \"date_time\": \"2015-04-09 17:55:17\", \"site_name\": 2, \"posa_continent\": 3, \"user_location_country\": 66, \"user_location_region\": 174, \"user_location_city\": 16634, \"orig_destination_distance\": 2635.7886, \"user_id\": 3706, \"is_mobile\": 0, \"is_package\": 0, \"channel\": 3, \"srch_ci\": \"2016-10-20\", \"srch_co\": \"2016-10-22\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 2, \"srch_rm_cnt\": 1, \"srch_destination_id\": 8811, \"srch_destination_type_id\": 1, \"hotel_id\": 1941325217798}";
        return Arrays.asList(value1, value2, value3, value4, value5, value6, value7, value8, value9);
    }

    private List<String> expectedValues() {
        String value1 = "{\"id\": 104,\"user_id\": 414, \"srch_ci\": \"2017-08-03\", \"srch_co\": \"2017-08-04\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 2, \"hotel_id\": 2310692405248}".replaceAll(" ", "");
        String value2 = "{\"id\": 260,\"user_id\": 951, \"srch_ci\": \"2016-10-20\", \"srch_co\": \"2016-10-21\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 0, \"hotel_id\": 1546188226565}".replaceAll(" ", "");
        String value3 = "{\"id\": 368,\"user_id\": 1277, \"srch_ci\": \"2017-08-30\", \"srch_co\": \"2017-08-31\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 1, \"hotel_id\": 3255585210372}".replaceAll(" ", "");
        String value4 = "{\"id\": 409,\"user_id\": 1383, \"srch_ci\": \"2017-08-29\", \"srch_co\": \"2017-09-01\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 0, \"hotel_id\": 1314259992578}".replaceAll(" ", "");
        String value5 = "{\"id\": 526,\"user_id\": 1640, \"srch_ci\": \"2016-10-18\", \"srch_co\": \"2016-10-20\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 0, \"hotel_id\": 1623497637889}".replaceAll(" ", "");
        String value6 = "{\"id\": 740,\"user_id\": 2458, \"srch_ci\": \"2016-10-26\", \"srch_co\": \"2016-10-30\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 0, \"hotel_id\": 429496729602}".replaceAll(" ", "");
        String value7 = "{\"id\": 762,\"user_id\": 2516, \"srch_ci\": \"2016-10-03\", \"srch_co\": \"2016-10-05\", \"srch_adults_cnt\": 1, \"srch_children_cnt\": 0, \"hotel_id\": 1632087572484}".replaceAll(" ", "");
        String value8 = "{\"id\": 944,\"user_id\": 3070, \"srch_ci\": \"2017-09-17\", \"srch_co\": \"2017-09-18\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 0, \"hotel_id\": 2482491097091}".replaceAll(" ", "");
        String value9 = "{\"id\": 1151,\"user_id\": 3706, \"srch_ci\": \"2016-10-20\", \"srch_co\": \"2016-10-22\", \"srch_adults_cnt\": 2, \"srch_children_cnt\": 2, \"hotel_id\": 1941325217798}".replaceAll(" ", "");
        return Arrays.asList(value1, value2, value3, value4, value5, value6, value7, value8, value9);
    }
}
