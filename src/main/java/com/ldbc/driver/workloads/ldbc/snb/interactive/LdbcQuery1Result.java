package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LdbcQuery1Result {
    private final long friendId;
    private final String friendLastName;
    private final int distanceFromPerson;
    private final long friendBirthday;
    private final long friendCreationDate;
    private final String friendGender;
    private final String friendBrowserUsed;
    private final String friendLocationIp;
    private final Iterable<String> friendEmails;
    private final Iterable<String> friendLanguages;
    private final String friendCityName;
    // (Person-studyAt->University.name,
    // Person-studyAt->.classYear,
    // Person-studyAt->University-isLocatedIn->City.name)
    private final Iterable<List<String>> friendUniversities;
    // (Person-workAt->Company.name,
    // Person-workAt->.workFrom,
    // Person-workAt->Company-isLocatedIn->City.name)
    private final Iterable<List<String>> friendCompanies;

    public LdbcQuery1Result(
            long friendId,
            String friendLastName,
            int distanceFromPerson,
            long friendBirthday,
            long friendCreationDate,
            String friendGender,
            String friendBrowserUsed,
            String friendLocationIp,
            Iterable<String> friendEmails,
            Iterable<String> friendLanguages,
            String friendCityName,
            Iterable<List<String>> friendUniversities,
            Iterable<List<String>> friendCompanies) {
        this.friendId = friendId;
        this.friendLastName = friendLastName;
        this.distanceFromPerson = distanceFromPerson;
        this.friendBirthday = friendBirthday;
        this.friendCreationDate = friendCreationDate;
        this.friendGender = friendGender;
        this.friendBrowserUsed = friendBrowserUsed;
        this.friendLocationIp = friendLocationIp;
        this.friendEmails = Sets.newHashSet(friendEmails);
        this.friendLanguages = Sets.newHashSet(friendLanguages);
        this.friendCityName = friendCityName;
        this.friendUniversities = Sets.newHashSet(friendUniversities);
        this.friendCompanies = Sets.newHashSet(friendCompanies);
    }

    public long friendId() {
        return friendId;
    }

    public String friendLastName() {
        return friendLastName;
    }

    public int distanceFromPerson() {
        return distanceFromPerson;
    }

    public long friendBirthday() {
        return friendBirthday;
    }

    public long friendCreationDate() {
        return friendCreationDate;
    }

    public String friendGender() {
        return friendGender;
    }

    public String friendBrowserUsed() {
        return friendBrowserUsed;
    }

    public String friendLocationIp() {
        return friendLocationIp;
    }

    public Iterable<String> friendEmails() {
        return friendEmails;
    }

    public Iterable<String> friendLanguages() {
        return friendLanguages;
    }

    public String friendCityName() {
        return friendCityName;
    }

    public Iterable<List<String>> friendUniversities() {
        return friendUniversities;
    }

    public Iterable<List<String>> friendCompanies() {
        return friendCompanies;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LdbcQuery1Result result = (LdbcQuery1Result) o;

        if (distanceFromPerson != result.distanceFromPerson) return false;
        if (friendBirthday != result.friendBirthday) return false;
        if (friendCreationDate != result.friendCreationDate) return false;
        if (friendId != result.friendId) return false;
        if (friendBrowserUsed != null ? !friendBrowserUsed.equals(result.friendBrowserUsed) : result.friendBrowserUsed != null)
            return false;
        if (friendCityName != null ? !friendCityName.equals(result.friendCityName) : result.friendCityName != null)
            return false;
        if (friendCompanies != null ? !Iterables.elementsEqual(sortListOfListsOfStrings(friendCompanies), sortListOfListsOfStrings(result.friendCompanies)) : result.friendCompanies != null)
            return false;
        if (friendEmails != null ? !Iterables.elementsEqual(sortStringList(friendEmails), sortStringList(result.friendEmails)) : result.friendEmails != null)
            return false;
        if (friendGender != null ? !friendGender.equals(result.friendGender) : result.friendGender != null)
            return false;
        if (friendLanguages != null ? !Iterables.elementsEqual(sortStringList(friendLanguages), sortStringList(result.friendLanguages)) : result.friendLanguages != null)
            return false;
        if (friendLastName != null ? !friendLastName.equals(result.friendLastName) : result.friendLastName != null)
            return false;
        if (friendLocationIp != null ? !friendLocationIp.equals(result.friendLocationIp) : result.friendLocationIp != null)
            return false;
        if (friendUniversities != null ? !Iterables.elementsEqual(sortListOfListsOfStrings(friendUniversities), sortListOfListsOfStrings(result.friendUniversities)) : result.friendUniversities != null)
            return false;
        return true;
    }

    private Iterable<String> sortStringList(Iterable<String> iterable) {
        List<String> list = Lists.newArrayList(iterable);
        Collections.sort(list);
        return list;
    }

    private List<List<String>> sortListOfListsOfStrings(Iterable<List<String>> iterable) {
        List<List<String>> list = Lists.newArrayList(iterable);
        Comparator<List<String>> threeElementStringListComparator = new Comparator<List<String>>() {
            @Override
            public int compare(List<String> o1, List<String> o2) {
                if (o1.size() != 3)
                    throw new RuntimeException("List must contain exactly three elements: " + o1);
                if (o2.size() != 3)
                    throw new RuntimeException("List must contain exactly three elements: " + o1);
                int result = o1.get(0).compareTo(o2.get(0));
                if (0 != result) return result;
                result = o1.get(1).compareTo(o2.get(1));
                if (0 != result) return result;
                return o1.get(2).compareTo(o2.get(2));
            }
        };
        Collections.sort(list, threeElementStringListComparator);
        return list;
    }

    @Override
    public String toString() {
        return "LdbcQuery1Result{" +
                "friendId=" + friendId +
                ", friendLastName='" + friendLastName + '\'' +
                ", distanceFromPerson=" + distanceFromPerson +
                ", friendBirthday=" + friendBirthday +
                ", friendCreationDate=" + friendCreationDate +
                ", friendGender='" + friendGender + '\'' +
                ", friendBrowserUsed='" + friendBrowserUsed + '\'' +
                ", friendLocationIp='" + friendLocationIp + '\'' +
                ", friendEmails=" + friendEmails +
                ", friendLanguages=" + friendLanguages +
                ", friendCityName='" + friendCityName + '\'' +
                ", friendUniversities=" + friendUniversities +
                ", friendCompanies=" + friendCompanies +
                '}';
    }
}
