package com.ldbc.driver.workloads.ldbc.snb.interactive;

import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class WriteEventStreamReaderTestData_NEW {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public static final String ROWS_FOR_ALL_EVENT_TYPES = "" +
            "42|ADD_PERSON|409|Lei|Zhao|male|1989-07-21|2011-01-18T08:36:04.000+0000|14.131.98.220|Chrome|392|english;swedish|user@email.com|1612|97,1|911,1970;935,1970;913,1971;1539,1971\n" +
            "42|ADD_LIKE_POST|1582|120207|2011-02-01T08:36:04.000+0000\n" +
            "42|ADD_LIKE_COMMENT|1095|120426|2011-01-24T05:44:13.000+0000\n" +
            "42|ADD_FORUM|2118|Group for The_Beekeeper in Pakistan|2011-01-03T06:04:47.000+0000|989|10716\n" +
            "42|ADD_FORUM_MEMBERSHIP|2153|372|2011-01-04T18:42:51.000+0000\n" +
            "42|ADD_POST|120343||2011-01-30T07:59:58.000+0000|91.229.229.89|Internet Explorer||" +
            "About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer.|" +
            "172|1673|2152|9|1437\n" +
            "42|ADD_COMMENT|4034293|2013-01-31T23:58:49.000+0000|200.11.32.131|Firefox|words|169|7460|91|-1|4034289|1403;1990;2009;2081;2817;2855;2987;6316;7425;8224;8466\n" +
            "42|ADD_COMMENT|4034293|2013-01-31T23:58:49.000+0000|200.11.32.131|Firefox|words|169|7460|91|-1|4034289|\n" +
            "42|ADD_FRIENDSHIP|1920|655|2011-01-10T15:58:45.000+0000\n";

    public static final String UPDATE_1_ADD_PERSON_ROW = "42|ADD_PERSON|409|Lei|Zhao|male|1989-07-21|2011-01-18T08:36:04.000+0000|14.131.98.220|Chrome|392|english;swedish|user@email.com|1612|97,1|911,1970;935,1970;913,1971;1539,1971";

    public static final String UPDATE_2_ADD_LIKE_POST_ROW = "42|ADD_LIKE_POST|1582|120207|2011-02-01T08:36:04.000+0000";

    public static final String UPDATE_3_ADD_LIKE_COMMENT = "42|ADD_LIKE_COMMENT|1095|120426|2011-01-24T05:44:13.000+0000";

    public static final String UPDATE_4_ADD_FORUM = "42|ADD_FORUM|2118|Group for The_Beekeeper in Pakistan|2011-01-03T06:04:47.000+0000|989|10716";

    public static final String UPDATE_5_ADD_FORUM_MEMBERSHIP = "42|ADD_FORUM_MEMBERSHIP|2153|372|2011-01-04T18:42:51.000+0000";

    public static final String UPDATE_6_ADD_POST =
            "42|ADD_POST|120343||2011-01-30T07:59:58.000+0000|91.229.229.89|Internet Explorer||" +
                    "About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer.|" +
                    "172|1673|2152|9|1437";

    public static final String UPDATE_6_ADD_POST_NO_TAGS =
            "42|ADD_POST|120343||2011-01-30T07:59:58.000+0000|91.229.229.89|Internet Explorer||" +
                    "About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer.|" +
                    "172|1673|2152|9|";

    public static final String UPDATE_7_ADD_COMMENT =
            "42|ADD_COMMENT|4034293|2013-01-31T23:58:49.000+0000|200.11.32.131|Firefox|words|169|7460|91|-1|4034289|1403;1990;2009;2081;2817;2855;2987;6316;7425;8224;8466";

    public static final String UPDATE_7_ADD_COMMENT_NO_TAGS =
            "42|ADD_COMMENT|4034293|2013-01-31T23:58:49.000+0000|200.11.32.131|Firefox|words|169|7460|91|-1|4034289|";

    public static final String UPDATE_8_ADD_FRIENDSHIP =
            "42|ADD_FRIENDSHIP|1920|655|2011-01-10T15:58:45.000+0000";
}
