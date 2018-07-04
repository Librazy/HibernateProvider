package org.librazy.provider.hibernate;

import cat.nyaa.nyaacore.database.DatabaseUtils;
import cat.nyaa.nyaacore.database.RelationalDB;
import cat.nyaa.nyaacore.database.TransactionalQuery;
import org.h2.jdbc.JdbcSQLException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class TransTest {
    private RelationalDB db;
    private RelationalDB db2;

    @BeforeClass
    public static void register() {
        DatabaseUtils.registerProvider("hibernate", new HibernateProvider());
    }

    @Before
    public void prepareDatabase() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("username", "sa");
        conf.put("password", "");
        conf.put("jdbc", "org.h2.Driver");
        conf.put("url", "jdbc:h2:mem:app_db;");
        Class<?>[] classes = new Class<?>[]{TestTable.class};
        conf.put("classes", classes);
        conf.put("dialect", "org.hibernate.dialect.H2Dialect");
        db = DatabaseUtils.get("hibernate", null, conf).connect();
        db2 = DatabaseUtils.get("hibernate", null, conf).connect();
        db.auto(TestTable.class).delete();
    }

    @After
    public void closeDatabase() {
        db.close();
        db2.close();
    }

    @Test
    public void testTransInsert(){
        try(TransactionalQuery<TestTable> query = db.transaction(TestTable.class)){
            query.insert(new TestTable(1L, "test", UUID.randomUUID(), UUID.randomUUID()));
            query.insert(new TestTable(2L, "test", UUID.randomUUID(), UUID.randomUUID()));
        }
        assertEquals(2, db.auto(TestTable.class).count());
    }

    @Test
    public void testTransInsertRollbackedByNonSqlEx(){
        try(TransactionalQuery<TestTable> query = db.transaction(TestTable.class, true)){
            query.insert(new TestTable(1L, "test", UUID.randomUUID(), UUID.randomUUID()));
            query.insert(new TestTable(2L, "test", UUID.randomUUID(), UUID.randomUUID()));
            db.auto(TestTable.class).insert(new TestTable(3L, "test", UUID.randomUUID(), UUID.randomUUID()));
            db.query(TestTable.class).insert(new TestTable(4L, "test", UUID.randomUUID(), UUID.randomUUID()));
            throwException();
            query.commit();
        } catch (Exception ignored){
        }
        List<TestTable> select = db.auto(TestTable.class).select();
        assertEquals(0, select.size());
        assertEquals(0, db.auto(TestTable.class).count());
    }

    private static void throwException() {
        throw new RuntimeException();
    }

    @Test
    public void testTransInsertRollbackedBySqlEx(){
        try(TransactionalQuery<TestTable> query = db.transaction(TestTable.class)){
            query.insert(new TestTable(1L, "test", UUID.randomUUID(), UUID.randomUUID()));
            query.insert(new TestTable(2L, "test", UUID.randomUUID(), UUID.randomUUID()));
            db.auto(TestTable.class).insert(new TestTable(3L, "test", UUID.randomUUID(), UUID.randomUUID()));
            db.auto(TestTable.class).insert(new TestTable(4L, "test", UUID.randomUUID(), UUID.randomUUID()));
            //Throws SQLiteException because of comparator
            query.where("string", " throw ", "test").delete();
        } catch (Exception e){
            assertEquals(SQLException.class, e.getCause().getClass());
        }
        assertEquals(0, db.auto(TestTable.class).count());
    }

    @Test
    public void testTransInsertNotVisibleBeforeCommit(){
        db.auto(TestTable.class).delete();
        try(TransactionalQuery<TestTable> query = db.transaction(TestTable.class)){
            query.insert(new TestTable(1L, "test", UUID.randomUUID(), UUID.randomUUID()));
            query.insert(new TestTable(2L, "test", UUID.randomUUID(), UUID.randomUUID()));
            assertEquals(2, query.count());
            assertEquals(0, db2.auto(TestTable.class).count());
            assertEquals(2, db.auto(TestTable.class).count());
        }
        try(TransactionalQuery<TestTable> query2 = db2.transaction(TestTable.class)){
            query2.insert(new TestTable(3L, "test2", UUID.randomUUID(), UUID.randomUUID()));
            query2.insert(new TestTable(4L, "test2", UUID.randomUUID(), UUID.randomUUID()));
            assertEquals(4, db2.auto(TestTable.class).count());
            assertEquals(2, db.auto(TestTable.class).count());
            assertEquals(4, query2.count());
        }
        try(TransactionalQuery<TestTable> query3 = db2.transaction(TestTable.class, true)){
            query3.insert(new TestTable(5L, "test3", UUID.randomUUID(), UUID.randomUUID()));
            query3.insert(new TestTable(6L, "test3", UUID.randomUUID(), UUID.randomUUID()));
            assertEquals(2, query3.where("string", "=", "test3").count());
            assertEquals(0, db.auto(TestTable.class).where("string", "=", "test3").count());
            assertEquals(6, db2.auto(TestTable.class).count());
            assertEquals(4, db.auto(TestTable.class).count());
            assertEquals(6, query3.reset().count());
            throwException();
            query3.commit();
        } catch (Exception ignored){}
        assertEquals(4, db.auto(TestTable.class).count());
        assertEquals(4, db2.auto(TestTable.class).count());
    }
}
