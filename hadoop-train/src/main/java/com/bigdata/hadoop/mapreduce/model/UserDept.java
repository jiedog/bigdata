package com.bigdata.hadoop.mapreduce.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class UserDept implements Writable{
    private Integer flag ;
    private String userId;
    private String username;
    private String deptId;
    private String deptName;

    public UserDept() {
       super();
    }

    public UserDept(Integer flag, String userId, String username, String deptId, String deptName) {
        this.flag = flag;
        this.userId = userId;
        this.username = username;
        this.deptId = deptId;
        this.deptName = deptName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(flag);
        out.writeUTF(userId);
        out.writeUTF(username);
        out.writeUTF(deptId);
        out.writeUTF(deptName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.flag =in.readInt();
        this.userId=in.readUTF();
        this.username=in.readUTF();
        this.deptId=in.readUTF();
        this.deptName=in.readUTF();
    }

    public Integer getFlag() {
        return flag;
    }

    public void setFlag(Integer flag) {
        this.flag = flag;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getDeptId() {
        return deptId;
    }

    public void setDeptId(String deptId) {
        this.deptId = deptId;
    }

    public String getDeptName() {
        return deptName;
    }

    public void setDeptName(String deptName) {
        this.deptName = deptName;
    }

//    @Override
//    public String toString() {
//        return "UserDept{" +
//                "flag=" + flag +
//                ", userId='" + userId + '\'' +
//                ", username='" + username + '\'' +
//                ", deptId='" + deptId + '\'' +
//                ", deptName='" + deptName + '\'' +
//                '}';
//    }
        @Override
        public String toString() {
            return
                    flag +"\t"+
                    userId + '\t' +
                    username + '\t' +
                    deptId + '\t' +
                   deptName + '\t' +
                            "\n"
                   ;
        }
}
