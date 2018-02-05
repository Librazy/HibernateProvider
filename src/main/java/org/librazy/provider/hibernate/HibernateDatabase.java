package org.librazy.provider.hibernate;

import cat.nyaa.nyaacore.database.Database;
import cat.nyaa.nyaacore.database.Query;
import cat.nyaa.nyaacore.database.RelationalDB;
import com.google.common.collect.HashBasedTable;
import org.apache.commons.lang.Validate;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.metamodel.spi.MetamodelImplementor;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.hibernate.tool.hbm2ddl.SchemaUpdate;
import org.hibernate.tool.schema.TargetType;

import javax.persistence.criteria.*;
import java.util.*;

public class HibernateDatabase implements RelationalDB {
    private SessionFactory sessionFactory;
    private ServiceRegistry serviceRegistry;
    private final Properties properties;
    private List<Class<?>> classes;
    public HibernateDatabase(Properties properties, List<Class<?>> classes)
    {
        this.properties = properties;
        this.classes = new ArrayList<>(classes);
        rebuild();
    }

    private void rebuild() {
        if(sessionFactory != null) sessionFactory.close();
        Configuration hibernateConfig = new Configuration().setProperties(properties);
        if (classes != null) {
            for (Class<?> cls :
                    classes) {
                hibernateConfig.addAnnotatedClass(cls);
            }
        }
        serviceRegistry = new StandardServiceRegistryBuilder().applySettings(hibernateConfig.getProperties()).build();
        sessionFactory = hibernateConfig.buildSessionFactory(serviceRegistry);
    }

    @Override
    public <T> Query<T> query(Class<T> cls) {
        return new HibernateQuery<>(this, cls);
    }

    @Override
    public void createTable(Class<?> cls) {
        classes.add(cls);
        rebuild();
    }

    @Override
    public void updateTable(Class<?> cls) {
        MetadataSources metadataSources = new MetadataSources(serviceRegistry).addAnnotatedClass(cls);
        new SchemaUpdate().execute(EnumSet.of(TargetType.DATABASE), metadataSources.buildMetadata());
        rebuild();
    }

    @Override
    public void deleteTable(Class<?> cls) {
        MetadataSources metadataSources = new MetadataSources(serviceRegistry).addAnnotatedClass(cls);
        new SchemaExport().execute(EnumSet.of(TargetType.DATABASE), SchemaExport.Action.DROP, metadataSources.buildMetadata());
        classes.remove(cls);
        rebuild();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Database> T connect() {
        Validate.isTrue(sessionFactory.isOpen());
        return (T) this;
    }

    @Override
    public void close() {
        sessionFactory.close();
    }

    public class HibernateQuery<T> implements Query<T> {
        private HashBasedTable<String, String, Object> where = HashBasedTable.create();
        private final HibernateDatabase database;
        private Class<T> cls;
        private Session session;
        HibernateQuery(HibernateDatabase database, Class<T> cls){
            this.database = database;
            this.cls = cls;
            session = database.sessionFactory.openSession();
        }

        @Override
        public Query<T> clear() {
            where.clear();
            return this;
        }

        @Override
        public Query<T> whereEq(String columnName, Object obj) {
            return where(columnName, "=", obj);
        }

        @Override
        public Query<T> where(String columnName, String comparator, Object obj) {
            where.put(comparator, columnName, obj);
            return this;
        }

        @Override
        public void delete() {
            Transaction transaction = session.beginTransaction();
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaDelete<T> cd = cb.createCriteriaDelete(cls);
            Root<T> root = cd.from(cls);
            where.cellSet().forEach(cell ->{
                switch (Objects.requireNonNull(cell.getRowKey())){
                    case "=":{
                        cd.where(cb.equal(root.get(cell.getColumnKey()), cell.getValue()));
                    } break;
                    case ">":{
                        cd.where(cb.ge(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                    } break;
                    case "<":{
                        cd.where(cb.lt(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                    } break;
                }
            });
            session.createQuery(cd).executeUpdate();
            transaction.commit();
        }

        @Override
        public void insert(T t) {
            Transaction transaction = session.beginTransaction();
            session.persist(t);
            transaction.commit();
        }

        @Override
        public List<T> select() {
            CriteriaQuery<T> cq = createQuery();
            return session.createQuery(cq).getResultList();
        }

        @Override
        public T selectUnique() {
            CriteriaQuery<T> cq = createQuery();
            return session.createQuery(cq).uniqueResult();
        }

        private CriteriaQuery<T> createQuery() {
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<T> cq = cb.createQuery(cls);
            Root<T> root = cq.from(cls);
            where.cellSet().forEach(cell ->{
                switch (Objects.requireNonNull(cell.getRowKey())){
                    case ">":{
                        cq.where(cb.ge(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                    } break;
                    case "<":{
                        cq.where(cb.lt(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                    } break;
                    case "=":{
                        cq.where(cb.equal(root.get(cell.getColumnKey()), cell.getValue()));
                    } break;
                }
            });
            return cq;
        }

        @Override
        public int count() {
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<Long> q = cb.createQuery(Long.class);
            Root<T> root = q.from(cls);
            where.cellSet().forEach(cell ->{
                switch (Objects.requireNonNull(cell.getRowKey())){
                    case "<":{
                        q.where(cb.lt(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                    } break;
                    case ">":{
                        q.where(cb.ge(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                    } break;
                    case "=":{
                        q.where(cb.equal(root.get(cell.getColumnKey()), cell.getValue()));
                    } break;
                }
            });
            q.select(cb.count(root));
            return session.createQuery(q).uniqueResult().intValue();
        }

        @Override
        public void update(T t, String... columns) {
            Transaction transaction = session.beginTransaction();
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaUpdate<T> cu = cb.createCriteriaUpdate(cls);
            Root<T> root = cu.from(cls);
            where.cellSet().forEach(cell ->{
                switch (Objects.requireNonNull(cell.getRowKey())){
                    case "<":{
                        cu.where(cb.lt(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                    } break;
                    case "=":{
                        cu.where(cb.equal(root.get(cell.getColumnKey()), cell.getValue()));
                    } break;
                    case ">":{
                        cu.where(cb.ge(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                    } break;
                }
            });
            MetamodelImplementor metamodel = (MetamodelImplementor) sessionFactory.getMetamodel();
            ClassMetadata classMetadata = (ClassMetadata) metamodel.entityPersister(cls);
            List<String> cols = Arrays.asList(columns);
            String[] props = classMetadata.getPropertyNames();
            for(String prop: props){
                if(cols.isEmpty() || cols.contains(prop)){
                    cu.set(prop, classMetadata.getPropertyValue(t, prop));
                }
            }
            session.createQuery(cu).executeUpdate();
            transaction.commit();
        }

    }
}
