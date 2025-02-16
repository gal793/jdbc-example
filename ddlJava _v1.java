import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgresDDLCreator {

    /**
     * Пример метода, аналогичного функции public.ddlx_create_table(p_schema text, p_table text)
     * из вашего скрипта. Генерирует DDL для создания таблицы (без учёта constraint'ов,
     * так как они выносятся в ddlx_create_constraints).
     */
    public static String ddlxCreateTable(Connection conn, String schema, String table) throws SQLException {
        // 1. Соберём базовую информацию о том, что это за таблица (logged/unlogged/temporary, partitioned, и пр.)
        TableInfo tableInfo = getTableInfo(conn, schema, table);

        // 2. Собираем список определений столбцов
        List<String> columnDefs = getColumnDefinitions(conn, schema, table);

        // 3. Формируем финальную строку CREATE TABLE
        StringBuilder ddl = new StringBuilder();

        // Учитываем тип таблицы (обычная, UNLOGGED, TEMP, PARTITION и т.п.)
        // Примерно как в оригинальном скрипте:
        // SELECT relpersistence FROM pg_class ...
        // relpersistence = 'u' => UNLOGGED, 't' => TEMP, иначе обычная
        String tableTypePrefix = "";
        if ("u".equals(tableInfo.relpersistence)) {
            tableTypePrefix = "UNLOGGED ";
        } else if ("t".equals(tableInfo.relpersistence)) {
            tableTypePrefix = "TEMPORARY ";
        }

        // Если это partitioned table, структура может отличаться (partitioned by ...).
        // Ниже — пример общей конструкции:
        ddl.append("CREATE ")
           .append(tableTypePrefix)
           .append("TABLE ")
           .append(quoteIdentifier(schema)).append(".").append(quoteIdentifier(table))
           .append(" (");

        // 4. Вставляем определения столбцов
        for (int i = 0; i < columnDefs.size(); i++) {
            ddl.append("\n    ").append(columnDefs.get(i));
            if (i < columnDefs.size() - 1) {
                ddl.append(",");
            }
        }
        ddl.append("\n)");

        // Если это partitioned table (relkind = 'p'), надо добавить partition by
        if ("p".equals(tableInfo.relkind)) {
            // Для partitioned можно выяснить выражение PARTITION BY из pg_partitions/pg_inherits, 
            // но это уже специфичная логика. Здесь для примера:
            String partitionExpr = getPartitionExpression(conn, schema, table);
            ddl.append("\nPARTITION BY ").append(partitionExpr);
        }

        // Если таблица наследует от другой (INHERITS)
        String inheritsClause = getInheritsClause(conn, schema, table);
        if (!inheritsClause.isEmpty()) {
            ddl.append("\nINHERITS (").append(inheritsClause).append(")");
        }

        // Пример опций таблицы (WITH (oids=false) и т.д.), если это требуется
        // Ниже заглушка - при необходимости адаптируйте:
        String tableOptions = getTableOptions(conn, schema, table);
        if (!tableOptions.isEmpty()) {
            ddl.append("\nWITH (").append(tableOptions).append(")");
        }

        // Пример указания TABLESPACE, если нужно
        String tableSpace = getTableSpace(conn, schema, table);
        if (!tableSpace.isEmpty()) {
            ddl.append("\nTABLESPACE ").append(tableSpace);
        }

        ddl.append(";");

        return ddl.toString();
    }

    /**
     * Аналогично функции public.ddlx_create_constraints(p_schema text, p_table text).
     * Генерирует DDL для PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY и т.п.
     */
    public static String ddlxCreateConstraints(Connection conn, String schema, String table) throws SQLException {
        // В оригинале используется pg_constraint, pg_class, pg_attribute и т.д.
        // Собираем список constraint'ов и создаём соответствующие ALTER TABLE ... ADD CONSTRAINT ... выражения.
        StringBuilder ddl = new StringBuilder();

        String sql = 
            "SELECT conname, contype, pg_get_constraintdef(c.oid, true) as condef " +
            "FROM pg_constraint c " +
            "JOIN pg_class t ON t.oid = c.conrelid " +
            "JOIN pg_namespace n ON n.oid = t.relnamespace " +
            "WHERE n.nspname = ? " +
            "  AND t.relname = ? " +
            "  AND contype IN ('p','u','f','c') " +  // primary, unique, foreign, check
            "ORDER BY conname";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String conName = rs.getString("conname");
                    String conType = rs.getString("contype"); 
                    String conDef  = rs.getString("condef");

                    // Например: ALTER TABLE schema.table ADD CONSTRAINT name condef;
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

    /**
     * Аналогично функции public.ddlx_alter_table_defaults(p_schema text, p_table text).
     * Устанавливает DEFAULT для каждого столбца, у которого он определён.
     */
    public static String ddlxAlterTableDefaults(Connection conn, String schema, String table) throws SQLException {
        StringBuilder ddl = new StringBuilder();

        // Ищем столбцы с дефолтами в information_schema.columns
        String sql = 
            "SELECT column_name, column_default " +
            "FROM information_schema.columns " +
            "WHERE table_schema = ? " +
            "  AND table_name   = ? " +
            "  AND column_default IS NOT NULL " +
            "ORDER BY ordinal_position";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String colName = rs.getString("column_name");
                    String colDefault = rs.getString("column_default");

                    // Пример: ALTER TABLE schema.table ALTER COLUMN colName SET DEFAULT colDefault;
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

    /**
     * Аналогично функции public.ddlx_alter_owner(p_schema text, p_table text).
     * Узнаём владельца из pg_class и pg_roles, формируем ALTER TABLE ... OWNER TO ...
     */
    public static String ddlxAlterOwner(Connection conn, String schema, String table) throws SQLException {
        StringBuilder ddl = new StringBuilder();

        String sql = 
            "SELECT pg_get_userbyid(relowner) AS owner " +
            "FROM pg_class c " +
            "JOIN pg_namespace n ON n.oid = c.relnamespace " +
            "WHERE n.nspname = ? " +
            "  AND c.relname = ?";

        String ownerName = null;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    ownerName = rs.getString("owner");
                }
            }
        }

        if (ownerName != null && !ownerName.isEmpty()) {
            ddl.append("ALTER TABLE ")
               .append(quoteIdentifier(schema)).append(".").append(quoteIdentifier(table))
               .append(" OWNER TO ")
               .append(quoteIdentifier(ownerName))
               .append(";\n");
        }

        return ddl.toString();
    }

    /* ========================================================================= */
    /* Вспомогательные методы для выборки информации и формирования частей DDL  */
    /* ========================================================================= */

    /**
     * Извлекает базовую информацию о таблице:
     * - relkind (p = partitioned, r = обычная, t = TOAST, и т.д.),
     * - relpersistence (u = unlogged, t = temporary, по умолчанию 'пусто' = обычная),
     * - прочие детали при необходимости.
     */
    private static TableInfo getTableInfo(Connection conn, String schema, String table) throws SQLException {
        String sql = 
            "SELECT c.relkind, c.relpersistence " +
            "FROM pg_class c " +
            "JOIN pg_namespace n ON n.oid = c.relnamespace " +
            "WHERE n.nspname = ? AND c.relname = ?";

        TableInfo info = new TableInfo();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    info.relkind = rs.getString("relkind");          // например 'r' (обычная), 'p' (partitioned)
                    info.relpersistence = rs.getString("relpersistence"); // 'u' (unlogged), 't' (temp)
                }
            }
        }

        return info;
    }

    /**
     * Аналогично тому, как это делается в ddlx_create_table: 
     * соберём из information_schema.columns полный список объявлений столбцов.
     */
    private static List<String> getColumnDefinitions(Connection conn, String schema, String table) throws SQLException {
        List<String> columns = new ArrayList<>();

        String sql = 
            "SELECT column_name, data_type, is_nullable, character_maximum_length, numeric_precision, numeric_scale, " +
            "       is_identity, identity_generation, column_default " +
            "FROM information_schema.columns " +
            "WHERE table_schema = ? AND table_name = ? " +
            "ORDER BY ordinal_position";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String colName         = rs.getString("column_name");
                    String dataType        = rs.getString("data_type");
                    String isNullable      = rs.getString("is_nullable");
                    Integer charLength     = (Integer) rs.getObject("character_maximum_length");
                    Integer numPrecision   = (Integer) rs.getObject("numeric_precision");
                    Integer numScale       = (Integer) rs.getObject("numeric_scale");
                    String isIdentity      = rs.getString("is_identity");
                    String identityGen     = rs.getString("identity_generation");  // ALWAYS / BY DEFAULT
                    String columnDefault   = rs.getString("column_default");

                    // Формируем тип. Например, если data_type = "character varying", то смотрим на длину.
                    // Если numeric — на precision/scale и т.д.
                    String columnType = buildColumnType(dataType, charLength, numPrecision, numScale);

                    // Если столбец является identity
                    String identityClause = "";
                    if ("YES".equalsIgnoreCase(isIdentity)) {
                        // В PG 12+ "GENERATED ALWAYS AS IDENTITY" или "GENERATED BY DEFAULT AS IDENTITY"
                        if ("ALWAYS".equalsIgnoreCase(identityGen)) {
                            identityClause = " GENERATED ALWAYS AS IDENTITY";
                        } else {
                            identityClause = " GENERATED BY DEFAULT AS IDENTITY";
                        }
                    }

                    // Формируем окончательное определение столбца
                    StringBuilder colDef = new StringBuilder();
                    colDef.append(quoteIdentifier(colName)).append(" ").append(columnType);

                    // Если isIdentity != YES, то проверим serial / bigserial (зависит от column_default),
                    // но обычно сейчас лучше пользоваться identity. В оригинальном скрипте есть куча проверок —
                    // тут для упрощения сделаем напрямую через identityClause.
                    if (!identityClause.isEmpty()) {
                        colDef.append(identityClause);
                    }

                    // NOT NULL
                    if ("NO".equalsIgnoreCase(isNullable)) {
                        colDef.append(" NOT NULL");
                    }

                    // В ddlx_create_table по умолчанию вынос default в отдельную функцию (ddlx_alter_table_defaults).
                    // Если вы хотите *не* выносить default отдельно, можно включить:
                    // if (columnDefault != null && !identityClause.isEmpty()) { ... }
                    // Но, по аналогии с исходным скриптом, defaults оформляются через ddlx_alter_table_defaults.

                    columns.add(colDef.toString());
                }
            }
        }

        return columns;
    }

    /**
     * Собирает выражение PARTITION BY (если нужно). 
     * Здесь упростим до примера: вернём «RANGE (some_column)» или «LIST (...)»
     * в зависимости от реальной структуры partitioned table.
     */
    private static String getPartitionExpression(Connection conn, String schema, String table) throws SQLException {
        // Логика может быть сложной, так как partitioning хранится в pg_partitioned_table, pg_inherits и т.д.
        // Ниже — упрощённый пример:
        return "RANGE (some_column)"; 
    }

    /**
     * Проверяем, наследует ли таблица от какой-то другой. 
     * В pg_inherits хранятся наследуемые родительские таблицы.
     * Возвращаем строку вида "parent_schema.parent_table" (или несколько).
     */
    private static String getInheritsClause(Connection conn, String schema, String table) throws SQLException {
        String sql = 
            "SELECT pn.nspname as parent_schema, pc.relname as parent_table " +
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
                    String parentSchema = rs.getString("parent_schema");
                    String parentTable  = rs.getString("parent_table");
                    parents.add(quoteIdentifier(parentSchema) + "." + quoteIdentifier(parentTable));
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
     * Дополнительные опции (WITH (...)) для таблицы.
     * В старых версиях PG это могли быть настройки fillfactor, autovacuum, oids и т.д.
     */
    private static String getTableOptions(Connection conn, String schema, String table) throws SQLException {
        // Для примера вернём что-то вроде "fillfactor=100".
        // В реальности надо смотреть pg_class.reloptions или pg_options_to_table().
        return "";
    }

    /**
     * Определение tablespace (если не дефолтный). 
     * В pg_class есть reltablespace, в pg_tablespace — spcname.
     */
    private static String getTableSpace(Connection conn, String schema, String table) throws SQLException {
        // Возвращайте spcname, если reltablespace != 0 и spcname не pg_default
        return "";
    }

    /**
     * Логика определения SQL-типа столбца на основе данных из information_schema.
     * Упрощённый пример (вы расширите, если в вашем скрипте больше проверок).
     */
    private static String buildColumnType(String dataType, Integer charLength, Integer numericPrecision, Integer numericScale) {
        // Например, для character varying
        if ("character varying".equalsIgnoreCase(dataType) || "varchar".equalsIgnoreCase(dataType)) {
            if (charLength != null) {
                return "varchar(" + charLength + ")";
            } else {
                return "varchar";
            }
        }

        // Для numeric
        if ("numeric".equalsIgnoreCase(dataType)) {
            if (numericPrecision != null && numericScale != null) {
                return "numeric(" + numericPrecision + "," + numericScale + ")";
            } else if (numericPrecision != null) {
                return "numeric(" + numericPrecision + ")";
            } else {
                return "numeric";
            }
        }

        // Прочие случаи (date, timestamp, text и т.д.)
        // Можно расширить логику при необходимости
        return dataType;
    }

    /**
     * Экранирование идентификаторов в двойные кавычки (на случай, если имя содержит заглавные буквы или спецсимволы).
     */
    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    /**
     * Вспомогательный класс для хранения информации о pg_class.
     */
    private static class TableInfo {
        String relkind;        // 'r', 'p', ...
        String relpersistence; // 'u', 't', ...
    }

    /* ========================================================================= */
    /* Пример использования                                                      */
    /* ========================================================================= */
    public static void main(String[] args) {
        // Предположим, что у вас есть Connection conn, schema="public", table="mytable"
        // Ниже — пример вызовов:

        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/testdb", "postgres", "password")) {

            String schema = "public";
            String table = "mytable";

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
