package org.wcp.flink.source.pojotype;

public class User {
    public Integer userId;
    public String userName;
    public User(Integer userId, String userName){
        this.userId =  userId;
        this.userName = userName;
    }
    public String toString(){
        return "{userId: " + userId + ", userName: " + userName + "}";
    }
}
