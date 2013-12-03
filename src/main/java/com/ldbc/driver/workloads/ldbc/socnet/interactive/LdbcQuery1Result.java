package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import java.util.Arrays;
import java.util.Collection;

public class LdbcQuery1Result
{
    private final String firstName;
    private final String lastName;
    private final long birthday;
    private final long creationDate;
    private final String gender;
    private final String[] languages;
    private final String browser;
    private final String ip;
    private final String[] emails;
    private final String personCity;
    private final Collection<String> unis;
    private final Collection<String> companies;

    public LdbcQuery1Result( String firstName, String lastName, long birthday, long creationDate, String gender,
            String[] languages, String browser, String ip, String[] emails, String personCity, Collection<String> unis,
            Collection<String> companies )
    {
        super();
        this.firstName = firstName;
        this.lastName = lastName;
        this.birthday = birthday;
        this.creationDate = creationDate;
        this.gender = gender;
        this.languages = languages;
        this.browser = browser;
        this.ip = ip;
        this.emails = emails;
        this.personCity = personCity;
        this.unis = unis;
        this.companies = companies;
    }

    public String firstName()
    {
        return firstName;
    }

    public String lastName()
    {
        return lastName;
    }

    public long birthday()
    {
        return birthday;
    }

    public long creationDate()
    {
        return creationDate;
    }

    public String gender()
    {
        return gender;
    }

    public String[] languages()
    {
        return languages;
    }

    public String browser()
    {
        return browser;
    }

    public String ip()
    {
        return ip;
    }

    public String[] emails()
    {
        return emails;
    }

    public String personCity()
    {
        return personCity;
    }

    public Collection<String> unis()
    {
        return unis;
    }

    public Collection<String> companies()
    {
        return companies;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) ( birthday ^ ( birthday >>> 32 ) );
        result = prime * result + ( ( browser == null ) ? 0 : browser.hashCode() );
        result = prime * result + ( ( companies == null ) ? 0 : companies.hashCode() );
        result = prime * result + (int) ( creationDate ^ ( creationDate >>> 32 ) );
        result = prime * result + Arrays.hashCode( emails );
        result = prime * result + ( ( firstName == null ) ? 0 : firstName.hashCode() );
        result = prime * result + ( ( gender == null ) ? 0 : gender.hashCode() );
        result = prime * result + ( ( ip == null ) ? 0 : ip.hashCode() );
        result = prime * result + Arrays.hashCode( languages );
        result = prime * result + ( ( lastName == null ) ? 0 : lastName.hashCode() );
        result = prime * result + ( ( personCity == null ) ? 0 : personCity.hashCode() );
        result = prime * result + ( ( unis == null ) ? 0 : unis.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj == null ) return false;
        if ( getClass() != obj.getClass() ) return false;
        LdbcQuery1Result other = (LdbcQuery1Result) obj;
        if ( birthday != other.birthday ) return false;
        if ( browser == null )
        {
            if ( other.browser != null ) return false;
        }
        else if ( !browser.equals( other.browser ) ) return false;
        if ( companies == null )
        {
            if ( other.companies != null ) return false;
        }
        else if ( !companies.equals( other.companies ) ) return false;
        if ( creationDate != other.creationDate ) return false;
        if ( !Arrays.equals( emails, other.emails ) ) return false;
        if ( firstName == null )
        {
            if ( other.firstName != null ) return false;
        }
        else if ( !firstName.equals( other.firstName ) ) return false;
        if ( gender == null )
        {
            if ( other.gender != null ) return false;
        }
        else if ( !gender.equals( other.gender ) ) return false;
        if ( ip == null )
        {
            if ( other.ip != null ) return false;
        }
        else if ( !ip.equals( other.ip ) ) return false;
        if ( !Arrays.equals( languages, other.languages ) ) return false;
        if ( lastName == null )
        {
            if ( other.lastName != null ) return false;
        }
        else if ( !lastName.equals( other.lastName ) ) return false;
        if ( personCity == null )
        {
            if ( other.personCity != null ) return false;
        }
        else if ( !personCity.equals( other.personCity ) ) return false;
        if ( unis == null )
        {
            if ( other.unis != null ) return false;
        }
        else if ( !unis.equals( other.unis ) ) return false;
        return true;
    }

    @Override
    public String toString()
    {
        return "LdbcQuery1Result [firstName=" + firstName + ", lastName=" + lastName + ", birthday=" + birthday
               + ", creationDate=" + creationDate + ", gender=" + gender + ", languages=" + Arrays.toString( languages )
               + ", browser=" + browser + ", ip=" + ip + ", emails=" + Arrays.toString( emails ) + ", personCity="
               + personCity + ", unis=" + unis + ", companies=" + companies + "]";
    }
}
