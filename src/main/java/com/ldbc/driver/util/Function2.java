package com.ldbc.driver.util;

public interface Function2<FROM1, FROM2, TO>
{
    TO apply( FROM1 from1, FROM2 from2 );
}
