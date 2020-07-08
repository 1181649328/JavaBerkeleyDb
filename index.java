import java.io.File;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;

/**
 * index
 */
public class index {
    private String dbEnvFilePath;
    private String databaseName;
    Environment myEnvironment = null;
    private Database weiboDatabase = null;

    public index(String dbEnvFilePath, String databaseName) {
        this.dbEnvFilePath = dbEnvFilePath;
        this.databaseName = databaseName;
        try {
            File file = new File(dbEnvFilePath);
            if (!file.exists()) {
                file.mkdirs();
                System.out.println("is not exists");
            }
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);

            myEnvironment = new Environment(file, envConfig);
            weiboDatabase = myEnvironment.openDatabase(null, databaseName, dbConfig);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 写入数据
    public boolean put(String key, String value, boolean isSync) {
        try {
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theValue = new DatabaseEntry(value.getBytes("UTF-8"));

            weiboDatabase.put(null, theKey, theValue);
            if (isSync) {
                this.sync();
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    // 同步数据到磁盘，持久化数据
    public boolean sync() {
        if (weiboDatabase != null) {
            try {
                weiboDatabase.sync();
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    public String getValue(String key) {
        try {
            DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
            DatabaseEntry theValue = new DatabaseEntry();
            weiboDatabase.get(null, theKey, theValue, LockMode.DEFAULT);
            if (theValue.getData() == null) {
                return null;
            }

            return new String(theValue.getData(), "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public boolean close() {
        try {
            // // 先关闭数据库
            if (weiboDatabase != null) {
                weiboDatabase.close();
            }
            // // 再关闭BDB系统环境变量
            if (myEnvironment != null) {
                myEnvironment.sync();
                myEnvironment.cleanLog(); // 在关闭环境前清理下日志
                myEnvironment.close();
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;

    }

    public static void main(String[] args) {
        String dbEnvFilePath = "bdb";
        String databaseName = "regList";
        String key = "tets";
        String value = "TESTvALUE";

        index dbd = new index(dbEnvFilePath, databaseName);
        dbd.put(key, value, true);
        dbd.getValue(key);
        System.out.println(dbd.getValue(key));

    }

}