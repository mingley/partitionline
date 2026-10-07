package org.slf4j.impl;

import org.slf4j.ILoggerFactory;
import org.slf4j.helpers.NOPLoggerFactory;

/**
 * Minimal static logger binder for standalone fixture generation.
 * Binds SLF4J to NOPLoggerFactory to avoid external logging dependencies
 * and ensure completely silent, deterministic fixture generation.
 */
public class StaticLoggerBinder {
    private static final StaticLoggerBinder SINGLETON = new StaticLoggerBinder();
    public static final String REQUESTED_API_VERSION = "1.7.36";
    private final ILoggerFactory loggerFactory = new NOPLoggerFactory();

    public static StaticLoggerBinder getSingleton() {
        return SINGLETON;
    }

    public ILoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    public String getLoggerFactoryClassStr() {
        return NOPLoggerFactory.class.getName();
    }
}
