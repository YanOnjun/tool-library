package personal.pyj.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 解析大规模SQL文件
 * @author pyj
 */
public class ImportSQLTool {

    private final String url;
    private final String username;
    private final String password;

    private final String driver;

    private final ExecutorService threadPool;

    private final int batchSize = 1000;

    private final int part = 4;

    public static void main(String[] args) throws InterruptedException {
        String url = "jdbc:mysql://localhost:3306/dbname?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&serverTimezone=GMT%2B8&allowPublicKeyRetrieval=true";
        String user = "root";
        String password = "root";
        String driver = "com.mysql.cj.jdbc.Driver";
        String sqlFilePath = "sqlfile.sql";
        ImportSQLTool importSQLTool = new ImportSQLTool(url, user, password, driver);
        importSQLTool.instance(sqlFilePath);
    }

    private ImportSQLTool(String url, String username, String password, String driver) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.driver = driver;
        this.threadPool = Executors.newFixedThreadPool(part);
    }

    public void instance(String sqlFilePath) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        LinkedBlockingQueue<String> dropQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<String> createQueue = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<String> insertQueue = new LinkedBlockingQueue<>();
        readSqlFile(sqlFilePath, dropQueue, createQueue, insertQueue);
        executeSql(dropQueue);
        executeSql(createQueue);
        executeSql(insertQueue);
        threadPool.shutdown();
        long endTime = System.currentTimeMillis();
        System.out.println("耗时秒：" + (endTime - startTime) * 1.0 / 1000 + "s");
    }

    private void readSqlFile(String sqlFilePath, LinkedBlockingQueue<String> dropQueue, LinkedBlockingQueue<String> createQueue, LinkedBlockingQueue<String> insertQueue) {
        try (BufferedReader br = new BufferedReader(new FileReader(sqlFilePath))) {
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {
                // 拼接单行 SQL 语句
                if (line.startsWith("/*")) {
                    while (line!=null && !line.endsWith("*/")) {
                        line = br.readLine();
                    }
                    continue;
                }
                sb.append(line);
                if (line.endsWith(";")) {
                    String sql = sb.toString();
                    // 添加进阻塞队列
                    if (sql.startsWith("DROP") || sql.startsWith("drop")) {
                        dropQueue.offer(sql);
                    } else if (sql.startsWith("CREATE") || sql.startsWith("create")) {
                        createQueue.offer(sql);
                    } else if (sql.startsWith("INSERT") || sql.startsWith("insert")) {
                        insertQueue.offer(sql);
                    }
                    sb.setLength(0);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void executeSql(LinkedBlockingQueue<String> queue){
        LinkedBlockingQueue<String>[] sqlQueues = new LinkedBlockingQueue[part];
        for (int i = 0; i < part; i++) {
            sqlQueues[i] = new LinkedBlockingQueue<>(batchSize);
        }

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < part; i++) {
            LinkedBlockingQueue<String> sqlQueue = sqlQueues[i];
            Future<?> future = threadPool.submit(() -> {
                try {
                    operationDB(sqlQueue);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            futures.add(future);
        }

        // 从阻塞队列中取出sql，放入四个队列中的其中一个
        // 生产
        while (queue.size() != 0) {
            try {
                String sql = queue.take();
                int index = Math.abs(sql.hashCode()) % 4;
                sqlQueues[index].put(sql);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        for(Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void operationDB(LinkedBlockingQueue<String> finalBatch) throws SQLException {
        Connection connection = null;
        Statement stmt = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
            connection.setAutoCommit(false);
            if (!connection.isValid(10)) {
                throw new SQLException("Database connection is invalid");
            }
            stmt = connection.createStatement();
            int count = 0;
            while(finalBatch.size()>0) {
                // 每次取出 batchSize 条 SQL 语句
                count ++;
                for (int i = 0; i < batchSize; i++) {
                    if (finalBatch.size() == 0) {
                        break;
                    }
                    String data = finalBatch.take();
                    stmt.addBatch(data);
                    if (data.length()> 256) {
                        data = data.substring(0, 256);
                    }
                    System.out.println(Thread.currentThread().getName() + "-添加：  " + data);
                }
                System.out.println(Thread.currentThread().getName() + "-第" + count + "次commit");
                stmt.executeBatch();
                connection.commit();
                System.out.println("===================== " + Thread.currentThread().getName() + "-第" + count + "次Commit完毕 =====================");
            }
        } catch (SQLException | ClassNotFoundException | InterruptedException | RuntimeException e) {
            System.out.println("===================== " + Thread.currentThread().getName() + "-执行失败 异常： =====================");
            e.printStackTrace();
        } finally {
            // 关闭 Statement 和 Connection 对象
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }
            System.out.println("===================== " + Thread.currentThread().getName() + "队列执行完毕 =====================");
            Thread.currentThread().interrupt();
        }
    }
}