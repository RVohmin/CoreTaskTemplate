package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {

    public void createUsersTable() throws SQLException {
        try (Connection connection = Util.getMySQLConnection()) {
            Statement statement = connection.createStatement();
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS users (id BIGINT NOT NULL AUTO_INCREMENT, name VARCHAR(50) NOT NULL, lastname VARCHAR(100) NOT NULL , age TINYINT UNSIGNED NOT NULL, PRIMARY KEY (id))");
        } catch (SQLException e) {
            throw new SQLException("Ошибка в методе createUsersTable()", e);
        }
    }

    public void dropUsersTable() throws SQLException {
        try (Connection connection = Util.getMySQLConnection()) {
            Statement statement = connection.createStatement();
            statement.executeUpdate("DROP TABLE if exists users");
        } catch (SQLException e) {
            throw new SQLException("Ошибка в методе dropUsersTable()", e);
        }
    }

    public void saveUser(String name, String lastName, byte age) throws SQLException {
        try (Connection connection = Util.getMySQLConnection()) {
            Statement statement = connection.createStatement();
            String sql = "INSERT into users(name, lastname, age) VALUES ('" + name + "', '" + lastName + "', " + age + ")";
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new SQLException("Ошибка в методе saveUser()", e);
        }
    }

    public void removeUserById(long id) throws SQLException {
        try (Connection connection = Util.getMySQLConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM users where id = ?");
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new SQLException("Ошибка в методе removeUserById()", e);
        }
    }

    public List<User> getAllUsers() throws SQLException {
        List<User> list = new ArrayList<>();
        try (Connection connection = Util.getMySQLConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM users");
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                User user = new User(resultSet.getString(2), resultSet.getString(3), (byte) resultSet.getInt(4));
                user.setId(resultSet.getLong(1));
                list.add(user);
            }
        } catch (SQLException e) {
            throw new SQLException("Ошибка в методе getAllUsers()", e);
        }
        return list;
    }

    public void cleanUsersTable() throws SQLException {
        try (Connection connection = Util.getMySQLConnection()) {
            Statement statement = connection.createStatement();
            statement.executeUpdate("DELETE FROM users");
        } catch (SQLException e) {
            throw new SQLException("Ошибка в методе cleanUsersTable()", e);
        }
    }
}
