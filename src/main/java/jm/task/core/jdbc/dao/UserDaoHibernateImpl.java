package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import java.util.List;
import java.util.function.Function;

public class UserDaoHibernateImpl implements UserDao {
    private final SessionFactory sessionFactory = Util.getSessionFactory();

    public UserDaoHibernateImpl() {
    }

    private <T> T tx(final Function<Session, T> command) {
        final Session session = sessionFactory.openSession();
        final Transaction tx = session.beginTransaction();
        try {
            T rsl = command.apply(session);
            tx.commit();
            return rsl;
        } catch (final Exception e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }

    @Override
    public void createUsersTable() {
        tx(session -> session.
                createSQLQuery("CREATE TABLE IF NOT EXISTS users (" +
                        "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                        "name VARCHAR(50) NOT NULL, lastname VARCHAR(100) NOT NULL , " +
                        "age TINYINT UNSIGNED NOT NULL)").executeUpdate());
    }

    @Override
    public void dropUsersTable() {
        tx(session -> session.createSQLQuery("DROP TABLE IF EXISTS users").executeUpdate());
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        tx(session -> session.save(new User(name, lastName, age)));
    }

    @Override
    public void removeUserById(long id) {
        tx(session -> {
                    final var query = session.createQuery("DELETE FROM User user where user.id = :userId");
                    query.setParameter("userId", id);
                    query.executeUpdate();
                    return null;
                }
        );
    }

    @Override
    public List<User> getAllUsers() {
        return tx(session -> session.createQuery("FROM User").list());
    }

    @Override
    public void cleanUsersTable() {
        tx(session -> session.createQuery("DELETE FROM User ").executeUpdate());
    }
}
