package org.librazy.provider.hibernate;

import cat.nyaa.nyaacore.database.DatabaseUtils;
import cat.nyaa.nyaacore.database.RelationalDB;
import cat.nyaa.nyaacore.database.TransactionalQuery;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class HibernateProviderTest {
    @BeforeClass
    public static void register() {
        DatabaseUtils.registerProvider("hibernate", new HibernateProvider());
    }

    @Test
    public void canConnect() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("username", "sa");
        conf.put("password", "");
        conf.put("jdbc", "org.h2.Driver");
        conf.put("url", "jdbc:h2:mem:app_db;");
        conf.put("dialect", "org.hibernate.dialect.H2Dialect");
        RelationalDB db = DatabaseUtils.get("hibernate", null, conf);
        db.connect();
        db.close();
    }

    @Test
    public void canCreateTable() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("username", "sa");
        conf.put("password", "");
        conf.put("jdbc", "org.h2.Driver");
        conf.put("url", "jdbc:h2:mem:app_db;");
        conf.put("dialect", "org.hibernate.dialect.H2Dialect");
        Class<?>[] classes = new Class<?>[]{TestEntity.class};
        conf.put("classes", classes);
        RelationalDB db = DatabaseUtils.get("hibernate", null, conf);
        db.connect();
        db.close();
    }

    @Test
    public void CanInsert() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("username", "sa");
        conf.put("password", "");
        conf.put("jdbc", "org.h2.Driver");
        conf.put("url", "jdbc:h2:mem:app_db;");
        conf.put("dialect", "org.hibernate.dialect.H2Dialect");
        Class<?>[] classes = new Class<?>[]{TestEntity.class};
        conf.put("classes", classes);
        RelationalDB db = DatabaseUtils.get("hibernate", null, conf);
        db.connect();
        TestEntity testEntity = new TestEntity().setTest("t1");
        db.auto(TestEntity.class).insert(testEntity);
        db.close();
    }

    @Test
    public void canSelectCount() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("username", "sa");
        conf.put("password", "");
        conf.put("jdbc", "org.h2.Driver");
        conf.put("url", "jdbc:h2:mem:app_db;");
        conf.put("dialect", "org.hibernate.dialect.H2Dialect");
        RelationalDB db = DatabaseUtils.get("hibernate", null, conf);
        db.connect();
        db.createTable(TestEntity.class);
        TestEntity testEntity = new TestEntity().setTest("t1");
        db.beginTransaction();
        db.query(TestEntity.class).delete();
        db.query(TestEntity.class).insert(testEntity);
        List<TestEntity> entities = db.query(TestEntity.class).select();
        Assert.assertEquals(1, entities.size());
        Assert.assertEquals(db.query(TestEntity.class).whereEq("test", "t1").count(), 1);
        db.close();
    }

    @Test
    public void canSelectWhere() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("username", "sa");
        conf.put("password", "");
        conf.put("jdbc", "org.h2.Driver");
        conf.put("url", "jdbc:h2:mem:app_db;");
        conf.put("dialect", "org.hibernate.dialect.H2Dialect");
        RelationalDB db = DatabaseUtils.get("hibernate", null, conf);
        db.connect();
        db.createTable(TestEntity.class);
        db.auto(TestEntity.class).delete();
        TestEntity testEntity = new TestEntity().setTest("t1");
        TestEntity testEntity2 = new TestEntity().setTest("t2");
        TestEntity testEntity3 = new TestEntity().setTest("t2");
        db.auto(TestEntity.class).insert(testEntity);
        db.auto(TestEntity.class).insert(testEntity2);
        db.auto(TestEntity.class).insert(testEntity3);
        List<TestEntity> entities = db.auto(TestEntity.class).select();
        Assert.assertEquals(entities.size(), 3);
        Assert.assertEquals(db.auto(TestEntity.class).whereEq("test", "t1").selectUnique().getTest(), "t1");
        db.close();
    }

    @Test
    public void canUpdateWhere() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("username", "sa");
        conf.put("password", "");
        conf.put("jdbc", "org.h2.Driver");
        conf.put("url", "jdbc:h2:mem:app_db;");
        conf.put("dialect", "org.hibernate.dialect.H2Dialect");
        RelationalDB db = DatabaseUtils.get("hibernate", null, conf);
        db.connect();
        db.createTable(TestEntity.class);
        TestEntity testEntity = new TestEntity().setTest("t1");
        TestEntity testEntity2 = new TestEntity().setTest("t2");
        TestEntity testEntity3 = new TestEntity().setTest("t2");
        TestEntity testEntity4 = new TestEntity().setTest("t3");
        UUID uid = UUID.randomUUID();
        TestEntity testEntity5 = new TestEntity().setTest("t2").setUuid(uid);
        db.beginTransaction();
        db.query(TestEntity.class).insert(testEntity);
        db.query(TestEntity.class).insert(testEntity2);
        db.query(TestEntity.class).insert(testEntity3);
        db.query(TestEntity.class).insert(testEntity4);
        db.query(TestEntity.class).whereEq("test", "t2").update(testEntity5);
        Assert.assertEquals(2, db.query(TestEntity.class).whereEq("test", "t2").count());
        db.commitTransaction();
        Assert.assertEquals(2, db.auto(TestEntity.class).whereEq("test", "t2").select().stream().filter(s -> s.getUuid().equals(uid)).count());
        db.close();
    }

    @Test
    public void canDeleteWhere() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("username", "sa");
        conf.put("password", "");
        conf.put("jdbc", "org.h2.Driver");
        conf.put("url", "jdbc:h2:mem:app_db;");
        conf.put("dialect", "org.hibernate.dialect.H2Dialect");
        RelationalDB db = DatabaseUtils.get("hibernate", null, conf);
        db.connect();
        db.createTable(TestEntity.class);
        TestEntity testEntity = new TestEntity().setTest("t1");
        TestEntity testEntity2 = new TestEntity().setTest("t2");
        TestEntity testEntity3 = new TestEntity().setTest("t2");
        TestEntity testEntity4 = new TestEntity().setTest("t3");
        try (TransactionalQuery<TestEntity> transaction = db.transaction(TestEntity.class)) {
            db.query(TestEntity.class).insert(testEntity);
            db.query(TestEntity.class).insert(testEntity2);
            db.query(TestEntity.class).insert(testEntity3);
            db.query(TestEntity.class).insert(testEntity4);
            db.query(TestEntity.class).whereEq("test", "t2").delete();
            Assert.assertEquals(db.query(TestEntity.class).whereEq("test", "t2").count(), 0);
            Assert.assertEquals(2, transaction.select().size());
        }
        db.close();
    }
}
