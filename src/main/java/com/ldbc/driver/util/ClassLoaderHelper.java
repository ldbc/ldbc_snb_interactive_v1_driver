package com.ldbc.driver.util;

import com.ldbc.driver.*;

import java.lang.reflect.Constructor;

public class ClassLoaderHelper {
    /**
     * DB
     */
    synchronized public static Db loadDb(String dbClassName) throws DbException {
        try {
            return loadDb(loadClass(dbClassName, Db.class));
        } catch (ClassLoadingException e) {
            throw new DbException(String.format("Error creating DB [%s]", dbClassName), e);
        }
    }

    synchronized public static Db loadDb(Class<? extends Db> dbClass) throws DbException {
        try {
            return dbClass.getConstructor().newInstance();
        } catch (Exception e) {
            throw new DbException(String.format("Error creating DB [%s]", dbClass.getName()), e);
        }
    }

    /**
     * Workload
     */
    synchronized public static Workload loadWorkload(String workloadClassName) throws WorkloadException {
        try {
            return loadWorkload(loadClass(workloadClassName, Workload.class));
        } catch (ClassLoadingException e) {
            throw new WorkloadException(String.format("Error creating Workload [%s]", workloadClassName), e);
        }
    }

    synchronized public static Workload loadWorkload(Class<? extends Workload> workloadClass) throws WorkloadException {
        try {
            return workloadClass.getConstructor().newInstance();
        } catch (Exception e) {
            throw new WorkloadException(String.format("Error creating Workload [%s]", workloadClass.getName()), e);
        }
    }

    /**
     * OperationHandler
     */
    synchronized public static Constructor<? extends OperationHandler> loadOperationHandlerConstructor(Class<? extends OperationHandler> operationHandlerClass) throws OperationException {
        try {
            Constructor<? extends OperationHandler> operationHandlerConstructor = operationHandlerClass.getConstructor();
            return operationHandlerConstructor;
        } catch (Exception e) {
            throw new OperationException(
                    String.format("Error creating OperationHandler [%s] with Operation [%s]", operationHandlerClass.getName()),
                    e);
        }
    }

    synchronized public static OperationHandler<?> loadOperationHandler(Class<? extends OperationHandler> operationHandlerClass) throws OperationException {
        try {
            OperationHandler<?> operationHandler = operationHandlerClass.getConstructor().newInstance();
            return operationHandler;
        } catch (Exception e) {
            throw new OperationException(
                    String.format("Error creating OperationHandler [%s] with Operation [%s]", operationHandlerClass.getName()),
                    e);
        }
    }

    /**
     * Helper Methods
     */
    synchronized public static <C> Class<? extends C> loadClass(String className, Class<C> baseClass) throws ClassLoadingException {
        try {
            ClassLoader classLoader = ClassLoaderHelper.class.getClassLoader();
            Class<?> loadedClass = classLoader.loadClass(className);
            // Class<?> loadedClass = Class.forName(className,false,classLoader)
            return (Class<? extends C>) loadedClass;
        } catch (ClassNotFoundException e) {
            throw new ClassLoadingException(String.format("Error loading class [%s]", className), e);
        }
    }

    synchronized public static Class<?> loadClass(String className) throws ClassLoadingException {
        try {
            ClassLoader classLoader = ClassLoaderHelper.class.getClassLoader();
            Class<?> loadedClass = classLoader.loadClass(className);
            return loadedClass;
        } catch (ClassNotFoundException e) {
            throw new ClassLoadingException(String.format("Error loading class [%s]", className), e);
        }
    }
}
