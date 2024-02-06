package org.wcp.flink.source.addsource.custom.richsourcefunction;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.wcp.flink.source.pojotype.User;

import java.sql.*;

public class MySQLReader extends RichSourceFunction<User> {
    private Connection connection = null;
    @Override
    public void open(Configuration configuration) {
        try {
            connection = DriverManager.getConnection("jdbc:mysql://localhost/wcp?user=flink&password=flink");
        }catch (Exception exception){
            System.out.println(exception.getMessage());
        }
    }
    @Override
    public void run(SourceContext<User> ctx) throws Exception {
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
