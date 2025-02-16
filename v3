import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgresDDLCreator {

    /**
     * Получаем числовую версию PostgreSQL:
     * например 90615 (9.6.15), 100002 (10.2), 120005 (12.5) и т.п.
     */
    private static int getServerVersionNum(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SHOW server_version_num")) {
            if (rs.next()) {
                return Integer.parseInt(rs.getString(1));
            }
        }
        return 0; // fallback, если вдруг не удалось прочесть
    }

    /* ========================================================================= */
    /* 1) Аналог public.ddlx_create_table(p_schema text, p_table text)           */
    /* ========================================================================= */
    public static String ddlxCreateTable(Connection conn, String schema, String table) throws SQLException {
        // Считываем версию, чтобы делать проверки типа #if PG_VERSION_GE(100000)
        int version = getServerVersionNum(conn);

        // 1. Получим базовую информацию о таблице (relkind, relpersistence).
        TableInfo tableInfo = getTableInfo(conn, schema, table);

        // 2. Собираем список столбцов. Внутри будет логика (SERIAL vs IDENTITY) с учётом версии.
        List<String> columnDefs = getColumnDefinitions(conn, schema, table, version);

        // 3. Формируем финальный DDL для CREATE TABLE
        StringBuilder ddl = new StringBuilder();

        // Определяем UNLOGGED / TEMP
        String tableTypePrefix = "";
        if ("u".equals(tableInfo.relpersistence)) {
            tableTypePrefix = "UNLOGGED ";
        } else if ("t".equals(tableInfo.relpersistence)) {
            tableTypePrefix = "TEMPORARY ";
        }

        // Начало CREATE TABLE
        ddl.append("CREATE ")
           .append(tableTypePrefix)
           .append("TABLE ")
           .append(quoteIdentifier(schema)).append(".").append(quoteIdentifier(table))
           .append(" (");

        // Добавляем определения столбцов
        for (int i = 0; i < columnDefs.size(); i++) {
            ddl.append("\n    ").append(columnDefs.get(i));
            if (i < columnDefs.size() - 1) {
                ddl.append(",");
            }
        }
        ddl.append("\n)");

        // Если PostgreSQL 10+ и таблица является partitioned (relkind='p'),
        // то добавляем PARTITION BY
        if (version >= 100000 && "p".equals(tableInfo.relkind)) {
            String partitionExpr = getPartitionExpression(conn, schema, table);
            ddl.append("\nPARTITION BY ").append(partitionExpr);
        }
        // Иначе, в 9.x, нативного partitioned (relkind='p') не было.

        // Если есть INHERITS
        String inheritsClause = getInheritsClause(conn, schema, table);
        if (!inheritsClause.isEmpty()) {
            ddl.append("\nINHERITS (").append(inheritsClause).append(")");
        }

        // Опции (WITH (...))
        String tableOptions = getTableOptions(conn, schema, table);
        if (!tableOptions.isEmpty()) {
            ddl.append("\nWITH (").append(tableOptions).append(")");
        }

        // TABLESPACE
        String tableSpace = getTableSpace(conn, schema, table);
        if (!tableSpace.isEmpty()) {
            ddl.append("\nTABLESPACE ").append(tableSpace);
        }

        ddl.append(";");

        return ddl.toString();
    }

    /* ========================================================================= */
    /* 2) Аналог public.ddlx_create_constraints(p_schema text, p_table text)     */
    /* ========================================================================= */
    public static String ddlxCreateConstraints(Connection conn, String schema, String table) throws SQLException {
        // Считываем версию, чтобы учесть #if PG_VERSION_GE(120000) или другие ветки
        int version = getServerVersionNum(conn);

        StringBuilder ddl = new StringBuilder();

        // Соберём все constraint'ы (p, u, f, c) и сформируем ALTER TABLE ... ADD CONSTRAINT
        // В некоторых версиях PG 12+ могут быть нюансы (например, CHECK ... NO INHERIT),
        // поэтому покажем ветку if/else как пример.
        String sql = 
            "SELECT conname, contype, pg_get_constraintdef(c.oid, true) AS condef " +
            "FROM pg_constraint c " +
            "JOIN pg_class t ON t.oid = c.conrelid " +
            "JOIN pg_namespace n ON n.oid = t.relnamespace " +
            "WHERE n.nspname = ? " +
            "  AND t.relname = ? " +
            "  AND contype IN ('p','u','f','c') " +
            "ORDER BY conname";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String conName = rs.getString("conname");
                    String conDef  = rs.getString("condef");  // например: CHECK ( ... ), FOREIGN KEY ( ... ), и т.д.

                    // Если хотим учесть PG12+ фичи (NO INHERIT, DEFERRABLE, и т.п.),
                    // обычно pg_get_constraintdef() уже это включает. Но допустим, у нас
                    // есть условие #if PG_VERSION_GE(120000). Для примера покажем:
                    if (version >= 120000) {
                        // PostgreSQL 12+ — допускаем, что conDef может содержать NO INHERIT
                        // (тут просто оставляем, как есть, pg_get_constraintdef уже учитывает)
                    } else {
                        // PG < 12
                        // возможно, у нас какая-то особая обработка, убираем "NO INHERIT" и т.д.
                        // (просто пример, реальная логика зависит от скрипта)
                        conDef = conDef.replace("NO INHERIT", "");
                    }

                    ddl.append("ALTER TABLE ")
                       .append(quoteIdentifier(schema)).append(".").append(quoteIdentifier(table))
                       .append(" ADD CONSTRAINT ")
                       .append(quoteIdentifier(conName)).append(" ")
                       .append(conDef).append(";\n");
                }
            }
        }

        return ddl.toString();
    }

    /* ========================================================================= */
    /* 3) Аналог public.ddlx_alter_table_defaults(p_schema text, p_table text)   */
    /* ========================================================================= */
    public static String ddlxAlterTableDefaults(Connection conn, String schema, String table) throws SQLException {
        int version = getServerVersionNum(conn);

        StringBuilder ddl = new StringBuilder();

        // В оригинальном скрипте есть #if PG_VERSION_GE(100000) в некоторых местах для defaults.
        // Обычно difference в том, что PG10+ может иметь identity-столбцы, но они не нуждаются в DEFAULT.
        // Однако, если хотим явно учесть, приводим пример.

        String sql = 
            "SELECT column_name, column_default " +
            "FROM information_schema.columns " +
            "WHERE table_schema = ? AND table_name = ? " +
            "  AND column_default IS NOT NULL " +
            "ORDER BY ordinal_position";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String colName = rs.getString("column_name");
                    String colDefault = rs.getString("column_default");

                    // Проверка на версии, если, например, хотим уберечься от "IDENTITY columns",
                    // которые в PG10+ вместо DEFAULT. 
                    // Для примера: если version < 100000, оставляем всё как есть;
                    // если version >= 100000, тоже обычно всё ок. 
                    // Но предположим, у нас логика: "не выставлять DEFAULT, если оно = nextval(...)"
                    // - чисто условно.
                    if (version >= 100000 && colDefault.contains("nextval(")) {
                        // Возможно, пропустим. (Это демонстрация, в реальном скрипте действуйте по ситуации.)
                        continue;
                    }

                    ddl.append("ALTER TABLE ")
                       .append(quoteIdentifier(schema)).append(".").append(quoteIdentifier(table))
                       .append(" ALTER COLUMN ")
                       .append(quoteIdentifier(colName))
                       .append(" SET DEFAULT ")
                       .append(colDefault)
                       .append(";\n");
                }
            }
        }

        return ddl.toString();
    }

    /* ========================================================================= */
    /* 4) Аналог public.ddlx_alter_owner(p_schema text, p_table text)            */
    /* ========================================================================= */
    public static String ddlxAlterOwner(Connection conn, String schema, String table) throws SQLException {
        // В вашем скрипте есть #if PG_VERSION_GE(90600). Для PG9.6+ иногда 
        // добавляют особый синтаксис, в более старых PG - другой. 
        // Здесь покажем ветку if/else:

        int version = getServerVersionNum(conn);

        StringBuilder ddl = new StringBuilder();

        // Для упрощения - метод получения реального владельца
        String ownerName = getTableOwner(conn, schema, table);
        if (ownerName == null || ownerName.isEmpty()) {
            return ""; // если не удалось определить владельца, ничего не делаем
        }

        // Если версия >= 9.6 (90600)
        if (version >= 90600) {
            // Обычный синтаксис ALTER TABLE .. OWNER TO
            ddl.append("ALTER TABLE ")
               .append(quoteIdentifier(schema)).append(".").append(quoteIdentifier(table))
               .append(" OWNER TO ")
               .append(quoteIdentifier(ownerName))
               .append(";\n");
        } else {
            // PG < 9.6, в вашем скрипте мог быть другой синтаксис или какой-то workaround
            // (в реальности синтаксис OWNER TO существует давно, но допустим).
            ddl.append("-- [PG < 9.6] ALTER TABLE OWNER: \n");
            ddl.append("ALTER TABLE ")
               .append(quoteIdentifier(schema)).append(".").append(quoteIdentifier(table))
               .append(" OWNER TO ")
               .append(quoteIdentifier(ownerName))
               .append(";\n");
        }

        return ddl.toString();
    }

    /* ========================================================================= */
    /* Вспомогательные методы (как в вашем большом коде)                         */
    /* ========================================================================= */

    /**
     * Содержит relkind (p, r и т.п.) и relpersistence ('u' = unlogged, 't' = temp)
     */
    private static class TableInfo {
        String relkind;        // 'r', 'p', ...
        String relpersistence; // 'u', 't', ...
    }

    /**
     * Определяем тип таблицы из pg_class + pg_namespace
     */
    private static TableInfo getTableInfo(Connection conn, String schema, String table) throws SQLException {
        TableInfo info = new TableInfo();
        String sql = 
            "SELECT c.relkind, c.relpersistence " +
            "FROM pg_class c " +
            "JOIN pg_namespace n ON n.oid = c.relnamespace " +
            "WHERE n.nspname = ? AND c.relname = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    info.relkind = rs.getString("relkind");
                    info.relpersistence = rs.getString("relpersistence");
                }
            }
        }
        return info;
    }

    /**
     * Извлекаем владельца таблицы
     */
    private static String getTableOwner(Connection conn, String schema, String table) throws SQLException {
        String sql = 
            "SELECT pg_get_userbyid(relowner) AS owner " +
            "FROM pg_class c " +
            "JOIN pg_namespace n ON n.oid = c.relnamespace " +
            "WHERE n.nspname = ? AND c.relname = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("owner");
                }
            }
        }
        return null;
    }

    /**
     * Собираем список определений столбцов с учётом:
     * - version >= 10 => использовать IDENTITY, иначе SERIAL
     * - NOT NULL
     * - прочие особенности
     */
    private static List<String> getColumnDefinitions(Connection conn, String schema, String table, int version) throws SQLException {
        List<String> columns = new ArrayList<>();

        // Проверим, есть ли поля is_identity и identity_generation в information_schema (это PG 10+).
        // В PG < 10 их нет, поэтому при version < 100000 игнорируем.
        // Чтобы упростить, сделаем запрос, где для PG<10 подставим NULL AS is_identity, NULL AS identity_generation.

        String identityFields = (version >= 100000)
            ? "is_identity, identity_generation,"
            : "NULL AS is_identity, NULL AS identity_generation,";

        String sql = 
            "SELECT column_name, data_type, is_nullable, " +
            "       character_maximum_length, numeric_precision, numeric_scale, " +
                    identityFields +
            "       column_default " +
            "FROM information_schema.columns " +
            "WHERE table_schema = ? AND table_name = ? " +
            "ORDER BY ordinal_position";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String colName       = rs.getString("column_name");
                    String dataType      = rs.getString("data_type");
                    String isNullable    = rs.getString("is_nullable");
                    Integer charLen      = (Integer) rs.getObject("character_maximum_length");
                    Integer numPrec      = (Integer) rs.getObject("numeric_precision");
                    Integer numScale     = (Integer) rs.getObject("numeric_scale");
                    String isIdentity    = rs.getString("is_identity");         // "YES"/"NO" (или null)
                    String identityGen   = rs.getString("identity_generation"); // "ALWAYS"/"BY DEFAULT" (или null)
                    String colDefault    = rs.getString("column_default");

                    // Определяем базовый тип (varchar(...), numeric(...), и т.п.)
                    String columnType = buildColumnType(dataType, charLen, numPrec, numScale);

                    // --- Проверяем автоинкремент (IDENTITY или SERIAL) ---
                    String identityClause = "";
                    boolean usedSerial = false;

                    if (version >= 100000) {
                        // PG 10+: если is_identity=YES, делаем IDENTITY
                        if ("YES".equalsIgnoreCase(isIdentity)) {
                            if ("ALWAYS".equalsIgnoreCase(identityGen)) {
                                identityClause = " GENERATED ALWAYS AS IDENTITY";
                            } else {
                                identityClause = " GENERATED BY DEFAULT AS IDENTITY";
                            }
                        }
                        // Если is_identity=NO, но column_default содержит nextval(...), тогда возможно это "старый" SERIAL
                        else if (colDefault != null && colDefault.matches(".*nextval\\(.*\\).*")) {
                            // Упрощённо определим SERIAL vs BIGSERIAL
                            // (в реальном коде чаще смотрят, какой у столбца тип: integer/bigint)
                            if ("bigint".equalsIgnoreCase(dataType)) {
                                columnType = "bigserial";
                            } else if ("integer".equalsIgnoreCase(dataType)) {
                                columnType = "serial";
                            }
                            usedSerial = true;
                        }
                    } else {
                        // PG < 10: нет is_identity
                        // Если colDefault ~ nextval(...), значит SERIAL/BIGSERIAL
                        if (colDefault != null && colDefault.matches(".*nextval\\(.*\\).*")) {
                            if ("bigint".equalsIgnoreCase(dataType)) {
                                columnType = "bigserial";
                            } else if ("integer".equalsIgnoreCase(dataType)) {
                                columnType = "serial";
                            }
                            usedSerial = true;
                        }
                    }

                    // Собираем итоговое объявление столбца
                    StringBuilder colDef = new StringBuilder();
                    colDef.append(quoteIdentifier(colName)).append(" ").append(columnType);

                    if (!identityClause.isEmpty()) {
                        colDef.append(identityClause);
                    }

                    // NOT NULL (общая логика для всех версий)
                    if ("NO".equalsIgnoreCase(isNullable)) {
                        colDef.append(" NOT NULL");
                    }

                    // В вашем исходном скрипте DEFAULT выносите в отдельную функцию ddlx_alter_table_defaults,
                    // так что здесь **не** добавляем "DEFAULT ...", если это не IDENTITY/SERIAL.
                    // Но если хотите, можете условно добавить.

                    columns.add(colDef.toString());
                }
            }
        }

        return columns;
    }

    /**
     * Если таблица является partitioned (relkind='p'), вернём PARTITION BY ...
     * В PostgreSQL 10+ есть pg_partitioned_table, и т.д.  
     * Здесь — упрощённая заглушка.
     */
    private static String getPartitionExpression(Connection conn, String schema, String table) throws SQLException {
        // В реальном коде вы бы делали SELECT из pg_partitioned_table и pg_attribute,
        // но здесь для примера вернём "RANGE (some_column)".
        return "RANGE (some_column)";
    }

    /**
     * Проверяем, наследует ли таблица от другой через pg_inherits.
     */
    private static String getInheritsClause(Connection conn, String schema, String table) throws SQLException {
        String sql = 
            "SELECT pn.nspname AS parent_schema, pc.relname AS parent_table " +
            "FROM pg_inherits i " +
            "JOIN pg_class c ON c.oid = i.inhrelid " +
            "JOIN pg_class pc ON pc.oid = i.inhparent " +
            "JOIN pg_namespace pn ON pn.oid = pc.relnamespace " +
            "JOIN pg_namespace cn ON cn.oid = c.relnamespace " +
            "WHERE cn.nspname = ? AND c.relname = ?";

        List<String> parents = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String pSchema = rs.getString("parent_schema");
                    String pTable  = rs.getString("parent_table");
                    parents.add(quoteIdentifier(pSchema) + "." + quoteIdentifier(pTable));
                }
            }
        }
        if (parents.isEmpty()) {
            return "";
        } else {
            return String.join(", ", parents);
        }
    }

    /**
     * Если нужно, возвращаем строку с опциями (WITH (...)).
     */
    private static String getTableOptions(Connection conn, String schema, String table) throws SQLException {
        // В оригинальном скрипте вызывалась ddlx_create_table_storage_parameters,
        // которая проверяла pg_class.reloptions, etc.  
        // Тут делаем заглушку (верните что нужно).
        return "";
    }

    /**
     * Если нужно указать TABLESPACE (если он не по умолчанию).
     */
    private static String getTableSpace(Connection conn, String schema, String table) throws SQLException {
        // В pg_class.relTablespace != 0 => смотрим в pg_tablespace.spcname.
        // Заглушка:
        return "";
    }

    /**
     * Упрощённая логика определения типа (varchar(...), numeric(...), и т.п.).
     * Без учёта SERIAL — ведь мы решаем это в коде выше (где проверяем column_default).
     */
    private static String buildColumnType(String dataType, Integer charLength, Integer numericPrecision, Integer numericScale) {
        // character varying / varchar
        if ("character varying".equalsIgnoreCase(dataType) || "varchar".equalsIgnoreCase(dataType)) {
            if (charLength != null) {
                return "varchar(" + charLength + ")";
            } else {
                return "varchar";
            }
        }
        // numeric
        if ("numeric".equalsIgnoreCase(dataType)) {
            if (numericPrecision != null && numericScale != null) {
                return "numeric(" + numericPrecision + "," + numericScale + ")";
            } else if (numericPrecision != null) {
                return "numeric(" + numericPrecision + ")";
            } else {
                return "numeric";
            }
        }
        // ... при необходимости допишите другие типы
        return dataType;
    }

    /**
     * Экранируем идентификаторы в двойные кавычки, 
     * чтобы корректно обрабатывать заглавные буквы, спецсимволы и т.п.
     */
    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /* ========================================================================= */
    /* Пример использования                                                      */
    /* ========================================================================= */
    public static void main(String[] args) {
        // Пример, как всё вызвать
        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/testdb", "postgres", "password")) {

            String schema = "public";
            String table  = "mytable";

            // 1) CREATE TABLE
            String createTableDDL = ddlxCreateTable(conn, schema, table);
            System.out.println("-- CREATE TABLE DDL --");
            System.out.println(createTableDDL);

            // 2) CREATE CONSTRAINTS
            String createConstraintsDDL = ddlxCreateConstraints(conn, schema, table);
            System.out.println("-- CREATE CONSTRAINTS DDL --");
            System.out.println(createConstraintsDDL);

            // 3) ALTER TABLE DEFAULTS
            String alterDefaultsDDL = ddlxAlterTableDefaults(conn, schema, table);
            System.out.println("-- ALTER TABLE DEFAULTS DDL --");
            System.out.println(alterDefaultsDDL);

            // 4) ALTER OWNER
            String alterOwnerDDL = ddlxAlterOwner(conn, schema, table);
            System.out.println("-- ALTER OWNER DDL --");
            System.out.println(alterOwnerDDL);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
