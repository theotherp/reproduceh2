import java.io.File;
import java.sql.*;
import java.util.Random;
import java.util.Scanner;
import java.util.stream.Stream;

public class Start {

    private static Random random = new Random();

    public static void main(String args[]) throws Exception {
        File databaseFile = new File("reproduce.mv.db");
        databaseFile.delete();

        Class.forName("org.h2.Driver");
        try (Connection con = DriverManager.getConnection("jdbc:h2:file:./reproduce", "sa", "")) {
            System.out.println();
            initializeDatabase(con);


            createIndexer(con, 1);
            createIndexer(con, 2);
            createIndexer(con, 3);

            createResults(con, 1);
            createResults(con, 2);
            createResults(con, 3);

            System.out.println("File size after adding all results: " + (databaseFile.length() / 1024 * 1024));

            printNumberOfResults(con);


            deleteIndexer(con, 1);
            System.out.println("File size after deleting indexer: " + (databaseFile.length() / 1024 * 1024));

            createIndexer(con, 4);
            createResults(con, 4);
            deleteIndexer(con, 2);

        }
        System.out.println("Press any key to finish");
        Scanner s = new Scanner(System.in);
        s.next();

    }

    private static void deleteIndexer(Connection con, int indexerId) throws SQLException {
        try (Statement statement = con.createStatement()) {
            statement.executeUpdate("delete from indexer where id = "+ indexerId);
        }
    }

    private static void createIndexer(Connection con, int index) throws SQLException {
        try (Statement statement = con.createStatement()) {
            statement.executeUpdate("insert into indexer values ( " + index + ", 'indexer" + index + "',0 )");
        }
    }

    private static void printNumberOfResults(Connection con) throws SQLException {
        try (Statement statement = con.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select count(*) from searchresult");
            resultSet.next();
            System.out.println("Number of results in db: " + resultSet.getInt(1));
        }
    }

    private static void createResults(Connection con, int indexerId) throws SQLException {
        for (int i = 1; i <= 2_000; i++) {
            try (PreparedStatement preparedStatement = con.prepareStatement("insert into searchresult (details, download_type, first_found, indexer_id, indexerguid, link, pub_date, title, id) values (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                for (int j = 1; j <= 100; j++) {
                    String resultId = indexerId + "-" + i + random.nextInt();
                    preparedStatement.setString(1, resultId); //details
                    preparedStatement.setString(2, "downloadType");
                    preparedStatement.setTimestamp(3, new Timestamp(2019, 6, 6, 6, 6, 6, 6));
                    preparedStatement.setInt(4, indexerId); //IndexerId
                    preparedStatement.setString(5, resultId); //indexerGuid
                    preparedStatement.setString(6, resultId); //link
                    preparedStatement.setTimestamp(7, new Timestamp(2019, 6, 6, 6, 6, 6, 6));
                    preparedStatement.setString(8, resultId); //title
                    preparedStatement.setLong(9, SearchResultIdCalculator.calculateSearchResultId(String.valueOf(random.nextInt()), "title-" + i + "-" + j, "link-" + i + "-" + j)); //id
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                if (i % 50 == 0) {
                    System.out.println("Executed batch " + i);
                }
            }
        }

    }


    private static void initializeDatabase(Connection con) throws SQLException {
        Statement stmt = con.createStatement();

        String initialize = "CREATE TABLE INDEXER\n" +
                "(\n" +
                "    ID        INTEGER PRIMARY KEY NOT NULL,\n" +
                "    NAME      VARCHAR(255),\n" +
                "    STATUS_ID INTEGER,\n" +
                ");\n" +
                "CREATE UNIQUE INDEX UK_XIFS7FHUVN4UB7IGF11FQEP0_INDEX_9\n" +
                "    ON INDEXER (NAME);\n" +
                "\n" +
                "\n" +
                "CREATE TABLE SEARCHRESULT\n" +
                "(\n" +
                "    ID            BIGINT PRIMARY KEY NOT NULL,\n" +
                "    DETAILS       VARCHAR(4000),\n" +
                "    DOWNLOAD_TYPE VARCHAR(255),\n" +
                "    FIRST_FOUND   TIMESTAMP,\n" +
                "    INDEXERGUID   VARCHAR(255)       NOT NULL,\n" +
                "    LINK          VARCHAR(4000),\n" +
                "    PUB_DATE      TIMESTAMP,\n" +
                "    TITLE         VARCHAR(4000)      NOT NULL,\n" +
                "    INDEXER_ID    INTEGER            NOT NULL,\n" +
                "    CONSTRAINT FKR5G21PDW3HHS1SEFVJY30TGMI FOREIGN KEY (INDEXER_ID) REFERENCES INDEXER (ID)\n" +
                ");\n" +
                "CREATE UNIQUE INDEX UKFTFA80663URIMM78EPNXHYOM_INDEX_C\n" +
                "    ON SEARCHRESULT (INDEXER_ID, INDEXERGUID);\n" +
                "\n" +
                "CREATE SEQUENCE PUBLIC.hibernate_sequence;\n" +
                "\n" +
                "ALTER TABLE SEARCHRESULT\n" +
                "    DROP CONSTRAINT FKR5G21PDW3HHS1SEFVJY30TGMI;\n" +
                "ALTER TABLE SEARCHRESULT\n" +
                "    ADD CONSTRAINT FKR5G21PDW3HHS1SEFVJY30TGMI\n" +
                "        FOREIGN KEY (INDEXER_ID) REFERENCES INDEXER (ID) ON DELETE CASCADE;\n" +
                "\n" +
                "\n" +
                "CREATE TABLE INDEXERNZBDOWNLOAD\n" +
                "(\n" +
                "    ID              INTEGER PRIMARY KEY NOT NULL,\n" +
                "    ACCESS_SOURCE   VARCHAR(255),\n" +
                "    AGE             INTEGER,\n" +
                "    ERROR           VARCHAR(255),\n" +
                "    EXTERNAL_ID     VARCHAR(255),\n" +
                "    NZB_ACCESS_TYPE VARCHAR(255),\n" +
                "    STATUS          VARCHAR(255),\n" +
                "    TIME            TIMESTAMP,\n" +
                "    TITLE           VARCHAR(4000),\n" +
                "    USERNAME_OR_IP  VARCHAR(255),\n" +
                "    INDEXER_ID      INTEGER,\n" +
                "    SEARCH_RESULT_ID BIGINT,\n" +
                "    CONSTRAINT FKMTRRF4HK98C9O3FDQJTS3HUPB FOREIGN KEY (INDEXER_ID) REFERENCES INDEXER (ID)\n" +
                ");\n" +
                "CREATE INDEX NZB_DOWNLOAD_EXT_ID\n" +
                "    ON INDEXERNZBDOWNLOAD (EXTERNAL_ID);\n" +
                "CREATE INDEX INDEXERNZBDOWNLOAD_INDEXER_ID_TIME_INDEX\n" +
                "    ON INDEXERNZBDOWNLOAD (INDEXER_ID, TIME DESC);\n" +
                "\n" +
                "ALTER TABLE SEARCHRESULT\n" +
                "    DROP CONSTRAINT FKR5G21PDW3HHS1SEFVJY30TGMI;\n" +
                "ALTER TABLE SEARCHRESULT\n" +
                "    ADD CONSTRAINT FKR5G21PDW3HHS1SEFVJY30TGMI\n" +
                "        FOREIGN KEY (INDEXER_ID) REFERENCES INDEXER (ID) ON DELETE CASCADE;\n" +
                "\n" +
                "ALTER TABLE INDEXERNZBDOWNLOAD\n" +
                "    ADD CONSTRAINT FKR5G21PDW3HHS1SEFKHD3HBDGL\n" +
                "        FOREIGN KEY (SEARCH_RESULT_ID) REFERENCES SEARCHRESULT (ID) ON DELETE CASCADE;";

        Stream.of(initialize.split(";")).forEach(x -> {
            try {
                stmt.executeUpdate(x);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        stmt.close();
    }

}
