package com.ldbc.driver.workloads.ldbc.snb.interactive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class InteractiveWriteEventStreamReaderTestData
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public static final String ROWS_FOR_ALL_EVENT_TYPES = "" +
            "42|666|1|409|Lei|Zhao|male|1234567890|1234567890|14.131.98.220|Chrome|392|english;swedish|user@email.com|1612|97,1|911,1970;935,1970;913,1971;1539,1971\n" +
            "42|666|2|1582|120207|1234567890\n" +
            "42|666|3|1095|120426|1234567890\n" +
            "42|666|4|2118|Group for The_Beekeeper in Pakistan|1234567890|989|10716\n" +
            "42|666|5|2153|372|1234567890\n" +
            "42|666|6|120343||1234567890|91.229.229.89|Internet Explorer||" +
            "About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer.|" +
            "172|1673|2152|9|1437\n" +
            "42|666|7|4034293|1234567890|200.11.32.131|Firefox|words|169|7460|91|-1|4034289|1403;1990;2009;2081;2817;2855;2987;6316;7425;8224;8466\n" +
            "42|666|7|4034293|1234567890|200.11.32.131|Firefox|words|169|7460|91|-1|4034289|\n" +
            "42|666|8|1920|655|1234567890\n";

    public static final String UPDATE_1_ADD_PERSON_ROW = "42|666|1|409|Lei|Zhao|male|1234567890|1234567890|14.131.98.220|Chrome|392|english;swedish|user@email.com|1612|97,1|911,1970;935,1970;913,1971;1539,1971";
    public static final String UPDATE_1_ADD_PERSON_ROW_ONE_LANGUAGE = "42|666|1|409|Lei|Zhao|male|1234567890|1234567890|14.131.98.220|Chrome|392|swedish|user@email.com|1612|97,1|911,1970;935,1970;913,1971;1539,1971";
    public static final String UPDATE_1_ADD_PERSON_ROW_NO_LANGUAGES = "42|666|1|409|Lei|Zhao|male|1234567890|1234567890|14.131.98.220|Chrome|392||user@email.com|1612|97,1|911,1970;935,1970;913,1971;1539,1971";
    public static final String UPDATE_1_ADD_PERSON_ROW_ONE_COMPANY = "42|666|1|409|Lei|Zhao|male|1234567890|1234567890|14.131.98.220|Chrome|392|english;swedish|user@email.com|1612|97,1|911,1970";
    public static final String UPDATE_1_ADD_PERSON_ROW_NO_COMPANIES = "42|666|1|409|Lei|Zhao|male|1234567890|1234567890|14.131.98.220|Chrome|392|english;swedish|user@email.com|1612|97,1|";
    public static final String UPDATE_1_ADD_PERSON_ROW_NO_UNIS = "42|666|1|409|Lei|Zhao|male|1234567890|1234567890|14.131.98.220|Chrome|392|english;swedish|user@email.com|1612||911,1970;935,1970;913,1971;1539,1971";
    public static final String UPDATE_1_ADD_PERSON_ROW_NO_EMAILS = "42|666|1|409|Lei|Zhao|male|1234567890|1234567890|14.131.98.220|Chrome|392|english;swedish||1612|97,1|911,1970;935,1970;913,1971;1539,1971";
    public static final String UPDATE_1_ADD_PERSON_ROW_NO_TAGS = "42|666|1|409|Lei|Zhao|male|1234567890|1234567890|14.131.98.220|Chrome|392|english;swedish|user@email.com||97,1|911,1970;935,1970;913,1971;1539,1971";

    public static final String UPDATE_2_ADD_LIKE_POST_ROW = "42|666|2|1582|120207|1234567890";

    public static final String UPDATE_3_ADD_LIKE_COMMENT = "42|666|3|1095|120426|1234567890";

    public static final String UPDATE_4_ADD_FORUM = "42|666|4|2118|Group for The_Beekeeper in Pakistan|1234567890|989|10716";
    // TODO tags

    public static final String UPDATE_5_ADD_FORUM_MEMBERSHIP = "42|666|5|2153|372|1234567890";

    public static final String UPDATE_6_ADD_POST =
            "42|666|6|120343||1234567890|91.229.229.89|Internet Explorer||" +
                    "About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer.|" +
                    "172|1673|2152|9|1437";

    public static final String UPDATE_6_ADD_POST_MANY_TAGS =
            "42|666|6|120343||1234567890|91.229.229.89|Internet Explorer||" +
                    "About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer.|" +
                    "172|1673|2152|9|1437;167;182";

    public static final String UPDATE_6_ADD_POST_NO_TAGS =
            "42|666|6|120343||1234567890|91.229.229.89|Internet Explorer||" +
                    "About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer.|" +
                    "172|1673|2152|9|";

    public static final String UPDATE_7_ADD_COMMENT =
            "42|666|7|4034293|1234567890|200.11.32.131|Firefox|words|169|7460|91|-1|4034289|1403;1990;2009;2081;2817;2855;2987;6316;7425;8224;8466";

    public static final String UPDATE_7_ADD_COMMENT_NO_TAGS =
            "42|666|7|4034293|1234567890|200.11.32.131|Firefox|words|169|7460|91|-1|4034289|";

    public static final String UPDATE_8_ADD_FRIENDSHIP =
            "42|666|8|1920|655|1234567890";
}
