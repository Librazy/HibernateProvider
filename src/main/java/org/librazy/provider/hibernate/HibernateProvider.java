package org.librazy.provider.hibernate;

import cat.nyaa.nyaacore.database.Database;
import cat.nyaa.nyaacore.database.DatabaseProvider;
import cat.nyaa.nyaacore.database.DatabaseUtils;
import org.bukkit.plugin.Plugin;

import javax.persistence.Entity;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HibernateProvider implements DatabaseProvider {
    @Override
    public Database get(Plugin plugin, Map<String, Object> map) {
        Properties props = new Properties();
        props.put("hibernate.dialect", map.get("dialect") == null ? "org.hibernate.dialect.MySQL57Dialect" : map.get("dialect"));
        props.put("hibernate.connection.driver_class", map.get("jdbc") == null ? "com.mysql.jdbc.Driver" : map.get("jdbc"));
        props.put("hibernate.connection.url", map.get("url"));
        props.put("hibernate.connection.username", map.get("username"));
        props.put("hibernate.connection.password", map.get("password"));
        props.put("hibernate.hbm2ddl.auto", "update");
        //props.put("hibernate.show_sql", "true");
        props.put("hibernate.jdbc.use_streams_for_binary", "true");
        props.put("hibernate.use_outer_join", "false");
        props.put("hibernate.jdbc.batch_size", "0");
        props.put("hibernate.jdbc.use_scrollable_resultset", "true");
        props.put("hibernate.statement_cache.size", "0");
        props.put("hibernate.current_session_context_class", "org.hibernate.context.internal.ThreadLocalSessionContext");
        props.put("hibernate.c3p0.timeout", "300");
        props.put("hibernate.c3p0.preferredTestQuery", "SELECT 1");
        props.put("hibernate.c3p0.testConnectionOnCheckout", "true");
        //props.put("hibernate.c3p0.unreturnedConnectionTimeout", "30");
        //props.put("hibernate.c3p0.debugUnreturnedConnectionStackTraces", "true");
        Logger.getLogger("org.hibernate").setLevel(Level.WARNING);
        Logger.getLogger("org.hibernate.SQL").setLevel(Level.WARNING);
        System.setProperty("com.mchange.v2.log.MLog", "com.mchange.v2.log.FallbackMLog");
        System.setProperty("com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL", "WARNING");
        Class<?>[] classes = plugin == null ? (Class<?>[]) map.get("classes") : DatabaseUtils.scanClasses(plugin, map, Entity.class);
        return new HibernateDatabase(props, Arrays.asList(classes == null? new Class<?>[0] : classes), plugin != null ? plugin.getLogger() : null);
    }
}
