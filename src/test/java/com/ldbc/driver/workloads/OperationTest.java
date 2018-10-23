package com.ldbc.driver.workloads;

import com.google.common.base.CaseFormat;
import com.google.common.collect.Sets;
import com.google.common.reflect.ClassPath;
import com.ldbc.driver.Operation;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class OperationTest
{
    public static <O extends Operation<?>> void assertCorrectParameterMap( O operation )
    {
        Map<String,Object> params = operation.parameterMap();

        Set<Field> commonFields = Sets.newHashSet( Operation.class.getDeclaredFields() );
        Set<Field> parameters = Stream.of( operation.getClass().getDeclaredFields() )
                .filter( field -> !Modifier.isStatic( field.getModifiers() ) )
                .filter( field -> !commonFields.contains( field ) )
                .collect( Collectors.toSet() );

        parameters.forEach( field ->
        {
            String fieldName = field.getName();
            Object expected;
            try
            {
                expected = operation.getClass().getDeclaredMethod( fieldName ).invoke( operation );
            }
            catch ( NoSuchMethodException | InvocationTargetException | IllegalAccessException e )
            {
                throw new RuntimeException( e );
            }
            final String keyName = params.keySet().stream()
                    .filter(k -> k.toLowerCase().equals(fieldName.toLowerCase()))
                    .findFirst().get();
            Object actual = params.get( keyName );

            assertThat(
                    "Expected " + operation.getClass().getName() + " to have parameter named (case-insensitive) '" + keyName + "'",
                    actual, is( notNullValue() ) );

            assertThat(
                    "Expected value of parameter (case-insensitive) '" + keyName + "' to be " + expected + ", " + "but was " + actual,
                    actual, equalTo( expected ) );
        } );
    }

    /**
     * Make sure that all operations implement static fields for parameter names
     */
    @Test
    public void allOperationsShouldProvideCorrectlyNamedStaticFieldsForAccessingTheirParametersMap() throws IOException
    {
        List<Class> operations = ClassPath.from( ClassLoader.getSystemClassLoader() )
                .getAllClasses().stream()
                .filter( classInfo -> classInfo.getPackageName().startsWith( "com.ldbc.driver" ) )
                .filter( classInfo -> !classInfo.getSimpleName().equals( "" ) ) // ignore anonymous classes
                .map( ClassPath.ClassInfo::load )
                .filter( clazz -> !Modifier.isAbstract( clazz.getModifiers() ) )
                .filter( Operation.class::isAssignableFrom )
                .collect( toList() );

        Set<Field> commonFields = Sets.newHashSet( Operation.class.getDeclaredFields() );

        operations.forEach( clazz ->
        {
            Set<Field> allFields = Sets.newHashSet( clazz.getDeclaredFields() );
            allFields.removeAll( commonFields );

            Stream<Field> nonStaticFields = allFields
                    .stream()
                    .filter( field -> !Modifier.isStatic( field.getModifiers() ) );

            nonStaticFields.forEach( field ->
            {
                String fieldName = field.getName().toLowerCase();
                assertTrue(
                        clazz.getName() + " is missing field name declaration (case-insensitive) " + fieldName + " for parameter " + field.getName(),
                        allFields.stream().anyMatch( f ->
                                CaseFormat.UPPER_UNDERSCORE
                                        .to( CaseFormat.LOWER_CAMEL, f.getName() )
                                        .toLowerCase()
                                        .equals(fieldName)
                        )
                );
            } );
        } );
    }
}
