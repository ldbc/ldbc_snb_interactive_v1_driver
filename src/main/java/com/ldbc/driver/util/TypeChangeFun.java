package com.ldbc.driver.util;

import com.google.common.base.Function;

import java.util.Map;

public abstract class TypeChangeFun<F, T> implements Function<F,T>
{
    public static final TypeChangeFun IDENTITY = new Identify();

    private static class Identify<T> extends TypeChangeFun<T,T>
    {
        @Override
        public T apply( T thing )
        {
            return thing;
        }
    }

    public static final TypeChangeFun TO_STRING = new IntToString();

    private static class IntToString<T> extends TypeChangeFun<T,String>
    {
        @Override
        public String apply( T thing )
        {
            return thing.toString();
        }
    }

    public static final <F, T> TypeChangeFun mapped( Map<F,T> mapping )
    {
        return new Mapped( mapping );
    }

    private static class Mapped<F, T> extends TypeChangeFun<F,T>
    {
        private final Map<F,T> mapping;

        private Mapped( Map<F,T> mapping )
        {
            this.mapping = mapping;
        }

        @Override
        public T apply( F thing )
        {
            return mapping.get( thing );
        }
    }
}
