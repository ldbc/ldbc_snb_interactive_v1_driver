package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

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
        this.friendEmails = friendEmails;
        this.friendLanguages = friendLanguages;
        this.friendCityName = friendCityName;
        this.friendUniversities = friendUniversities;
        this.friendCompanies = friendCompanies;
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

        LdbcQuery1Result other = (LdbcQuery1Result) o;

        if (distanceFromPerson != other.distanceFromPerson) return false;
        if (friendBirthday != other.friendBirthday) return false;
        if (friendCreationDate != other.friendCreationDate) return false;
        if (friendId != other.friendId) return false;
        if (friendBrowserUsed != null ? !friendBrowserUsed.equals(other.friendBrowserUsed) : other.friendBrowserUsed != null)
            return false;
        if (friendCityName != null ? !friendCityName.equals(other.friendCityName) : other.friendCityName != null)
            return false;
        if (friendCompanies != null ? !listOfListsOfStringsElementsEqual(sortListOfListsOfStrings(friendCompanies), sortListOfListsOfStrings(other.friendCompanies)) : other.friendCompanies != null)
            return false;
        if (friendEmails != null ? !Iterables.elementsEqual(sortStringList(friendEmails), sortStringList(other.friendEmails)) : other.friendEmails != null)
            return false;
        if (friendGender != null ? !friendGender.equals(other.friendGender) : other.friendGender != null)
            return false;
        if (friendLanguages != null ? !Iterables.elementsEqual(sortStringList(friendLanguages), sortStringList(other.friendLanguages)) : other.friendLanguages != null)
            return false;
        if (friendLastName != null ? !friendLastName.equals(other.friendLastName) : other.friendLastName != null)
            return false;
        if (friendLocationIp != null ? !friendLocationIp.equals(other.friendLocationIp) : other.friendLocationIp != null)
            return false;
        if (friendUniversities != null ? !listOfListsOfStringsElementsEqual(sortListOfListsOfStrings(friendUniversities), sortListOfListsOfStrings(other.friendUniversities)) : other.friendUniversities != null)
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

    private boolean listOfListsOfStringsElementsEqual(List<List<String>> listOfLists1, List<List<String>> listOfLists2) {
        if (listOfLists1.size() != listOfLists2.size()) return false;
        for (int i = 0; i < listOfLists1.size(); i++) {
            if (false == listOfLists1.get(i).equals(listOfLists2.get(i))) return false;
        }
        return true;
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
