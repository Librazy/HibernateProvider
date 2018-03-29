package org.librazy.provider.hibernate;

import cat.nyaa.nyaacore.database.Database;
import cat.nyaa.nyaacore.database.Query;
import cat.nyaa.nyaacore.database.RelationalDB;
import com.google.common.collect.HashBasedTable;
import org.apache.commons.lang.Validate;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.metamodel.spi.MetamodelImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.hibernate.tool.hbm2ddl.SchemaUpdate;
import org.hibernate.tool.schema.TargetType;

import javax.persistence.FlushModeType;
import javax.persistence.criteria.*;
import java.util.*;

public class HibernateDatabase implements RelationalDB {
    private SessionFactory sessionFactory;
    private ServiceRegistry serviceRegistry;
    private final Properties properties;
    private List<Class<?>> classes;
    private Session session;
    private Transaction transaction;

    HibernateDatabase(Properties properties, List<Class<?>> classes) {
        this.properties = properties;
        this.classes = new ArrayList<>(classes);
        rebuild();
    }

    private void rebuild() {
        if (sessionFactory != null) sessionFactory.close();
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
        return new HibernateQuery<>(this, cls, session, transaction);
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
    public synchronized void enableAutoCommit() {
        if (transaction != null) {
            session.flush();
            transaction.commit();
        } else {
            throw new IllegalStateException("No transaction found in this database");
        }
        session.close();
        session = null;
        transaction = null;
    }

    @Override
    public synchronized void disableAutoCommit() {
        if (transaction != null) {
            throw new IllegalStateException("Another transaction is in progress");
        }
        session = sessionFactory.openSession();
        session.setHibernateFlushMode(FlushMode.MANUAL);
        session.setFlushMode(FlushModeType.COMMIT);
        transaction = session.beginTransaction();
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
        private final HashBasedTable<String, String, Object> where = HashBasedTable.create();
        private final Class<T> cls;
        private final Session session;
        private final Transaction transaction;
        private Map<String, String> columnMapping = new HashMap<>();

        HibernateQuery(HibernateDatabase database, Class<T> cls, Session session, Transaction transaction) {
            this.cls = cls;
            this.transaction = transaction;
            if (session == null) {
                this.session = database.sessionFactory.openSession();
            } else {
                this.session = session;
            }
            MetamodelImplementor metamodel = (MetamodelImplementor) sessionFactory.getMetamodel();
            AbstractEntityPersister classMetadata = (AbstractEntityPersister) metamodel.entityPersister(cls);
            String[] props = classMetadata.getPropertyNames();
            String id = classMetadata.getIdentifierPropertyName();
            columnMapping.put(id, id);
            columnMapping.put(classMetadata.getIdentifierColumnNames()[0], id);
            for (String prop : props) {
                String[] names = classMetadata.getPropertyColumnNames(prop);
                columnMapping.put(names[0], prop);
                columnMapping.put(prop, prop);
            }
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
            Validate.notNull(columnMapping.get(columnName), "No suitable column or property found for '" + columnName + "'");
            where.put(comparator, columnMapping.get(columnName), obj);
            return this;
        }

        @Override
        public void delete() {
            Transaction transaction;
            if (this.transaction != null) {
                transaction = this.transaction;
            } else {
                transaction = session.beginTransaction();
            }
            try {
                CriteriaBuilder cb = session.getCriteriaBuilder();
                CriteriaDelete<T> cd = cb.createCriteriaDelete(cls);
                Root<T> root = cd.from(cls);
                where.cellSet().forEach(cell -> {
                    switch (Objects.requireNonNull(cell.getRowKey())) {
                        case "=": {
                            cd.where(cb.equal(root.get(cell.getColumnKey()), cell.getValue()));
                        }
                        break;
                        case ">": {
                            cd.where(cb.ge(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                        }
                        break;
                        case "<": {
                            cd.where(cb.lt(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                        }
                        break;
                    }
                });
                session.createQuery(cd).executeUpdate();
                if (this.transaction == null) {
                    transaction.commit();
                }
            } catch (Exception e) {
                transaction.rollback();
                throw e;
            } finally {
                session.close();
            }
        }

        @Override
        public void insert(T t) {
            Transaction transaction;
            if (this.transaction != null) {
                transaction = this.transaction;
            } else {
                transaction = session.beginTransaction();
            }
            try {
                session.persist(t);
                if (this.transaction == null) {
                    transaction.commit();
                }
            } catch (Exception e) {
                transaction.rollback();
                throw e;
            } finally {
                session.close();
            }
        }

        @Override
        public List<T> select() {
            try {
                CriteriaQuery<T> cq = createQuery();
                return session.createQuery(cq).getResultList();
            } finally {
                session.close();
            }
        }

        @Override
        public T selectUnique() {
            try {
                CriteriaQuery<T> cq = createQuery();
                return session.createQuery(cq).uniqueResult();
            } finally {
                session.close();
            }
        }

        private CriteriaQuery<T> createQuery() {
            CriteriaBuilder cb = session.getCriteriaBuilder();
            CriteriaQuery<T> cq = cb.createQuery(cls);
            Root<T> root = cq.from(cls);
            where.cellSet().forEach(cell -> {
                switch (Objects.requireNonNull(cell.getRowKey())) {
                    case ">": {
                        cq.where(cb.ge(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                    }
                    break;
                    case "<": {
                        cq.where(cb.lt(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                    }
                    break;
                    case "=": {
                        cq.where(cb.equal(root.get(cell.getColumnKey()), cell.getValue()));
                    }
                    break;
                }
            });
            return cq;
        }

        @Override
        public int count() {
            try {
                CriteriaBuilder cb = session.getCriteriaBuilder();
                CriteriaQuery<Long> q = cb.createQuery(Long.class);
                Root<T> root = q.from(cls);
                where.cellSet().forEach(cell -> {
                    switch (Objects.requireNonNull(cell.getRowKey())) {
                        case "<": {
                            q.where(cb.lt(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                        }
                        break;
                        case ">": {
                            q.where(cb.ge(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                        }
                        break;
                        case "=": {
                            q.where(cb.equal(root.get(cell.getColumnKey()), cell.getValue()));
                        }
                        break;
                    }
                });
                q.select(cb.count(root));
                return session.createQuery(q).uniqueResult().intValue();
            } finally {
                session.close();
            }
        }

        @Override
        public void update(T t, String... columns) {
            Transaction transaction;
            if (this.transaction != null) {
                transaction = this.transaction;
            } else {
                transaction = session.beginTransaction();
            }
            try {
                CriteriaBuilder cb = session.getCriteriaBuilder();
                CriteriaUpdate<T> cu = cb.createCriteriaUpdate(cls);
                Root<T> root = cu.from(cls);
                where.cellSet().forEach(cell -> {
                    switch (Objects.requireNonNull(cell.getRowKey())) {
                        case "<": {
                            cu.where(cb.lt(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                        }
                        break;
                        case "=": {
                            cu.where(cb.equal(root.get(cell.getColumnKey()), cell.getValue()));
                        }
                        break;
                        case ">": {
                            cu.where(cb.ge(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                        }
                        break;
                    }
                });
                MetamodelImplementor metamodel = (MetamodelImplementor) sessionFactory.getMetamodel();
                ClassMetadata classMetadata = (ClassMetadata) metamodel.entityPersister(cls);
                List<String> cols = Arrays.asList(columns);
                String[] props = classMetadata.getPropertyNames();
                for (String prop : props) {
                    if (cols.isEmpty() || cols.contains(prop)) {
                        cu.set(prop, classMetadata.getPropertyValue(t, prop));
                    }
                }
                session.createQuery(cu).executeUpdate();
                if (this.transaction == null) {
                    transaction.commit();
                }
            } catch (Exception e) {
                transaction.rollback();
                throw e;
            } finally {
                session.close();
            }
        }
    }
}
