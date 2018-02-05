package org.librazy.provider.hibernate;

import cat.nyaa.nyaacore.database.DatabaseUtils;
import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;

import java.util.Properties;
import java.util.logging.Level;

public final class Main extends JavaPlugin {

    @Override
    public void onEnable() {
        if(!DatabaseUtils.hasProvider("hibernate")){
            Bukkit.getLogger().log(Level.INFO, "Registering HibernateProvider");
            DatabaseUtils.registerProvider("hibernate", new HibernateProvider());
        }
    }

    @Override
    public void onDisable() {
        if(DatabaseUtils.hasProvider("hibernate")){
            Bukkit.getLogger().log(Level.INFO, "Unregistering HibernateProvider");
            DatabaseUtils.unregisterProvider("hibernate");
        }
    }

    @Override
    public void onLoad() {
        Bukkit.getLogger().log(Level.INFO, "Registering HibernateProvider");
        DatabaseUtils.registerProvider("hibernate", new HibernateProvider());
    }
}
