package org.wcp.flink.source.addsource.custom.sourcefunction;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.wcp.flink.source.pojotype.User;

import java.sql.*;

public class MySQLReader implements SourceFunction<User> {
    private Connection connection = null;
    @Override
    public void run(SourceContext<User> ctx) throws Exception {
        try {
            connection = DriverManager.getConnection("jdbc:mysql://localhost/wcp?user=flink&password=flink");
        }catch (Exception exception){
            System.out.println(exception.getMessage());
        }
        Statement statement = connection.createStatement();

        ResultSet resultSet = statement.executeQuery("select * from user");
        while (resultSet.next()){
            User user = new User(resultSet.getInt("user_id"), resultSet.getString("user_name"));
            ctx.collect(user);
        }
    }
    @Override
    public void cancel() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
