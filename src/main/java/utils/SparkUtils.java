package utils;

import org.apache.spark.sql.SparkSession;

public class SparkUtils {
    private static ThreadLocal<SparkSession> sessionPool = new ThreadLocal<>();

    public static SparkSession initSession() {
        if (sessionPool.get() != null) {
            return sessionPool.get();
        }

        SparkSession session = SparkSession.builder().appName("etl").master("local[*]")
                .config("hive.metastore.uris","thrift://node:9083")
                .config("es.nodes", "node")
                .config("es.port", "9200")
                .config("es.index.auto.cerate", "false")
                .enableHiveSupport()
                .getOrCreate();

        sessionPool.set(session);
        return session;
    }
}
