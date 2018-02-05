package org.librazy.provider.hibernate;

import cat.nyaa.nyaacore.database.DatabaseUtils;
import cat.nyaa.nyaacore.database.RelationalDB;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class HibernateProviderTest {
    @BeforeClass
    public static void register(){
        DatabaseUtils.registerProvider("hibernate", new HibernateProvider());
    }

    @Test
    public void canConnect() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("username", "sa");
        conf.put("password", "");
        conf.put("jdbc", "org.h2.Driver");
        conf.put("url", "jdbc:h2:mem:app_db;");
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
        Class<?>[] classes = new Class<?>[]{TestEntity.class};
        conf.put("classes", classes);
        RelationalDB db = DatabaseUtils.get("hibernate", null, conf);
        db.connect();
        TestEntity testEntity = new TestEntity().setTest("t1");
        db.query(TestEntity.class).insert(testEntity);
        db.close();
    }

    @Test
    public void canSelectCount() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("username", "sa");
        conf.put("password", "");
        conf.put("jdbc", "org.h2.Driver");
        conf.put("url", "jdbc:h2:mem:app_db;");
        conf.put("classes", null);
        RelationalDB db = DatabaseUtils.get("hibernate", null, conf);
        db.connect();
        db.createTable(TestEntity.class);
        TestEntity testEntity = new TestEntity().setTest("t1");
        db.query(TestEntity.class).insert(testEntity);
        List<TestEntity> entities = db.query(TestEntity.class).select();
        Assert.assertEquals(entities.size(), 1);
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
        conf.put("classes", null);
        RelationalDB db = DatabaseUtils.get("hibernate", null, conf);
        db.connect();
        db.createTable(TestEntity.class);
        TestEntity testEntity = new TestEntity().setTest("t1");
        TestEntity testEntity2 = new TestEntity().setTest("t2");
        TestEntity testEntity3 = new TestEntity().setTest("t2");
        db.query(TestEntity.class).insert(testEntity);
        db.query(TestEntity.class).insert(testEntity2);
        db.query(TestEntity.class).insert(testEntity3);
        List<TestEntity> entities = db.query(TestEntity.class).select();
        Assert.assertEquals(entities.size(), 3);
        Assert.assertEquals(db.query(TestEntity.class).whereEq("test", "t1").selectUnique().getTest(), "t1");
        db.close();
    }

    @Test
    public void canUpdateWhere() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("username", "sa");
        conf.put("password", "");
        conf.put("jdbc", "org.h2.Driver");
        conf.put("url", "jdbc:h2:mem:app_db;");
        conf.put("classes", null);
        RelationalDB db = DatabaseUtils.get("hibernate", null, conf);
        db.connect();
        db.createTable(TestEntity.class);
        TestEntity testEntity = new TestEntity().setTest("t1");
        TestEntity testEntity2 = new TestEntity().setTest("t2");
        TestEntity testEntity3 = new TestEntity().setTest("t2");
        TestEntity testEntity4 = new TestEntity().setTest("t3");
        UUID uid = UUID.randomUUID();
        TestEntity testEntity5 = new TestEntity().setTest("t2").setUuid(uid);
        db.query(TestEntity.class).insert(testEntity);
        db.query(TestEntity.class).insert(testEntity2);
        db.query(TestEntity.class).insert(testEntity3);
        db.query(TestEntity.class).insert(testEntity4);
        db.query(TestEntity.class).whereEq("test", "t2").update(testEntity5);
        Assert.assertEquals(db.query(TestEntity.class).whereEq("test", "t2").count(), 2);
        Assert.assertEquals(db.query(TestEntity.class).whereEq("test", "t2").select().stream().filter(s -> s.getUuid().equals(uid)).count(), 2);
        db.close();
    }

    @Test
    public void canDeleteWhere() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("username", "sa");
        conf.put("password", "");
        conf.put("jdbc", "org.h2.Driver");
        conf.put("url", "jdbc:h2:mem:app_db;");
        conf.put("classes", null);
        RelationalDB db = DatabaseUtils.get("hibernate", null, conf);
        db.connect();
        db.createTable(TestEntity.class);
        TestEntity testEntity = new TestEntity().setTest("t1");
        TestEntity testEntity2 = new TestEntity().setTest("t2");
        TestEntity testEntity3 = new TestEntity().setTest("t2");
        TestEntity testEntity4 = new TestEntity().setTest("t3");
        db.query(TestEntity.class).insert(testEntity);
        db.query(TestEntity.class).insert(testEntity2);
        db.query(TestEntity.class).insert(testEntity3);
        db.query(TestEntity.class).insert(testEntity4);
        db.query(TestEntity.class).whereEq("test", "t2").delete();
        Assert.assertEquals(db.query(TestEntity.class).whereEq("test", "t2").count(), 0);
        Assert.assertEquals(db.query(TestEntity.class).select().size(), 2);
        db.close();
    }
}
