package org.librazy.provider.hibernate;

import cat.nyaa.nyaacore.database.*;
import com.google.common.collect.HashBasedTable;
import org.apache.commons.lang.Validate;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.context.internal.ThreadLocalSessionContext;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.metamodel.spi.MetamodelImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.hibernate.tool.hbm2ddl.SchemaUpdate;
import org.hibernate.tool.schema.TargetType;

import javax.persistence.FlushModeType;
import javax.persistence.criteria.*;
import java.io.PrintStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Logger;

public class HibernateDatabase implements RelationalDB {
    private SessionFactory sessionFactory;
    private ServiceRegistry serviceRegistry;
    private final Properties properties;
    private List<Class<?>> classes;
    private Session session;
    private Transaction transaction;
    private Logger log = Logger.getLogger("HibernateProvider");
    HibernateDatabase(Properties properties, List<Class<?>> classes, Logger logger) {
        if(logger != null) log = logger;
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
        return new HibernateQuery<>(this, cls, false, false, session, transaction);
    }

    @Override
    public <T> TransactionalQuery<T> transaction(Class<T> cls) {
        return new HibernateQuery<>(this, cls, true, true, null, null);
    }

    @Override
    public <T> Query<T> auto(Class<T> cls) {
        if (session != null) {
            return new AutoQuery<>(new HibernateQuery<>(this, cls, false, false, session, transaction));
        } else {
            return new AutoQuery<>(new HibernateQuery<>(this, cls, true, false, null, null));
        }
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

    @Deprecated
    @Override
    public synchronized void commitTransaction() {
        if (transaction != null && transaction.isActive()) {
            session.flush();
            transaction.commit();
        } else {
            throw new IllegalStateException("No transaction found in this database or is not active");
        }
        session.close();
        session = null;
        transaction = null;
    }

    @Deprecated
    @Override
    public synchronized void beginTransaction() {
        if (transaction != null) {
            throw new IllegalStateException("Another transaction is in progress");
        }
        session = sessionFactory.openSession();
        session.setHibernateFlushMode(FlushMode.MANUAL);
        session.setFlushMode(FlushModeType.COMMIT);
        transaction = session.beginTransaction();
    }

    @Deprecated
    @Override
    public synchronized void rollbackTransaction() {
        if (transaction != null && transaction.isActive()) {
            transaction.setRollbackOnly();
            transaction.rollback();
        } else {
            throw new IllegalStateException("No transaction found in this database or is not active");
        }
        session.close();
        session = null;
        transaction = null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Database> T connect() {
        Validate.isTrue(sessionFactory.isOpen());
        return (T) this;
    }

    @Override
    public void close() {
        if (transaction != null && transaction.isActive()) {
            if (transaction.getRollbackOnly()) {
                transaction.rollback();
            } else {
                transaction.commit();
            }
        }
        if (session != null) {
            session.close();

        }
        sessionFactory.close();
    }

    public class HibernateQuery<T> implements TransactionalQuery<T> {
        private final HashBasedTable<String, String, Object> where = HashBasedTable.create();
        private final Class<T> cls;
        private final boolean managed;
        private final boolean bind;
        private final Session session;
        private final Transaction transaction;
        private Map<String, String> columnMapping = new HashMap<>();

        HibernateQuery(HibernateDatabase database, Class<T> cls, boolean newTrans, boolean bind, Session session, Transaction transaction) {
            this.cls = cls;
            this.managed = session != null;
            this.bind = bind;
            if (newTrans) {
                this.session = database.sessionFactory.openSession();
                if (bind) ThreadLocalSessionContext.bind(this.session);
                this.transaction = this.session.beginTransaction();
                this.session.setHibernateFlushMode(FlushMode.MANUAL);
                this.session.setFlushMode(FlushModeType.COMMIT);
            } else {
                this.session = session == null ? database.sessionFactory.getCurrentSession() : session;
                this.transaction = transaction == null ? this.session.getTransaction() : transaction;
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
        public TransactionalQuery<T> clear() {
            where.clear();
            return this;
        }

        @Override
        public TransactionalQuery<T> whereEq(String columnName, Object obj) {
            return where(columnName, "=", obj);
        }

        @Override
        public TransactionalQuery<T> where(String columnName, String comparator, Object obj) {
            Validate.notNull(columnMapping.get(columnName), "No suitable column or property found for '" + columnName + "'");
            where.put(comparator, columnMapping.get(columnName), obj);
            return this;
        }

        @Override
        public void rollback() {
            transaction.setRollbackOnly();
            transaction.rollback();
        }

        @Override
        public void commit() {
            transaction.commit();
        }

        @Override
        public void delete() {
            try {
                CriteriaBuilder cb = session.getCriteriaBuilder();
                CriteriaDelete<T> cd = cb.createCriteriaDelete(cls);
                Root<T> root = cd.from(cls);
                where.cellSet().forEach(cell -> {
                    switch (Objects.requireNonNull(cell.getRowKey())) {
                        case "=": {
                            Class<?> t = root.get(cell.getColumnKey()).type().getJavaType();
                            Class<?> s = Objects.requireNonNull(cell.getValue()).getClass();
                            if (t == UUID.class && s == String.class) {
                                cd.where(cb.equal(root.get(cell.getColumnKey()), UUID.fromString((String) cell.getValue())));
                            } else {
                                cd.where(cb.equal(root.get(cell.getColumnKey()), cell.getValue()));
                            }
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
            } catch (Exception e) {
                transaction.setRollbackOnly();
                throw e;
            }
        }

        @Override
        public void insert(T t) {
            try {
                session.persist(t);
                session.flush();
            } catch (Exception e) {
                transaction.setRollbackOnly();
                throw e;
            }
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

        @Override
        public T selectUniqueUnchecked() {
            CriteriaQuery<T> cq = createQuery();
            List<T> list = session.createQuery(cq).getResultList();
            if (list.size() != 1) {
                return null;
            }
            return list.get(0);
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
                        Class<?> t = root.get(cell.getColumnKey()).type().getJavaType();
                        Class<?> s = Objects.requireNonNull(cell.getValue()).getClass();
                        if (t == UUID.class && s == String.class) {
                            cq.where(cb.equal(root.get(cell.getColumnKey()), UUID.fromString((String) cell.getValue())));
                        } else {
                            cq.where(cb.equal(root.get(cell.getColumnKey()), cell.getValue()));
                        }
                    }
                    break;
                }
            });
            return cq;
        }

        @Override
        public int count() {
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
                        Class<?> t = root.get(cell.getColumnKey()).type().getJavaType();
                        Class<?> s = Objects.requireNonNull(cell.getValue()).getClass();
                        if (t == UUID.class && s == String.class) {
                            q.where(cb.equal(root.get(cell.getColumnKey()), UUID.fromString((String) cell.getValue())));
                        } else {
                            q.where(cb.equal(root.get(cell.getColumnKey()), cell.getValue()));
                        }
                    }
                    break;
                }
            });
            q.select(cb.count(root));
            return session.createQuery(q).uniqueResult().intValue();
        }

        @Override
        public void update(T t, String... columns) {
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
                            Class<?> j = root.get(cell.getColumnKey()).type().getJavaType();
                            Class<?> s = Objects.requireNonNull(cell.getValue()).getClass();
                            if (j == UUID.class && s == String.class) {
                                cu.where(cb.equal(root.get(cell.getColumnKey()), UUID.fromString((String) cell.getValue())));
                            } else {
                                cu.where(cb.equal(root.get(cell.getColumnKey()), cell.getValue()));
                            }
                        }
                        break;
                        case ">": {
                            cu.where(cb.ge(root.get(cell.getColumnKey()), (Number) cell.getValue()));
                        }
                        break;
                        case " LIKE ": {
                            cu.where(cb.like(root.get(cell.getColumnKey()), (String) cell.getValue()));
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
            } catch (Exception e) {
                transaction.setRollbackOnly();
                throw e;
            }
        }

        @Override
        public void close() {
            if (managed) return;
            if (bind) ThreadLocalSessionContext.unbind(sessionFactory);
            if (transaction.getRollbackOnly()) {
                transaction.rollback();
                session.close();
                return;
            }
            session.flush();
            transaction.commit();
            session.close();
        }
    }
}
