import java.sql.*;
import java.util.*;

public class DDLGenerator {
    private final Connection connection;

    public DDLGenerator(Connection connection) {
        this.connection = connection;
    }

    /**
     * Формирует DDL для создания таблицы (аналог ddlx_create_table)
     */
    public String ddlxCreateTable(String schema, String table) throws SQLException {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE ").append(schema).append(".").append(table).append(" (\n");

        // Получаем информацию о колонках из information_schema.columns
        String sql = "SELECT column_name, data_type, character_maximum_length, " +
                     "numeric_precision, numeric_scale, is_nullable, column_default " +
                     "FROM information_schema.columns " +
                     "WHERE table_schema = ? AND table_name = ? " +
                     "ORDER BY ordinal_position";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, table);
            try (ResultSet rs = stmt.executeQuery()) {
                List<String> columnDefs = new ArrayList<>();
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    // Может быть null, если тип не требует длины
                    Integer charLength = rs.getObject("character_maximum_length") != null ? rs.getInt("character_maximum_length") : null;
                    Integer numPrecision = rs.getObject("numeric_precision") != null ? rs.getInt("numeric_precision") : null;
                    Integer numScale = rs.getObject("numeric_scale") != null ? rs.getInt("numeric_scale") : null;
                    String isNullable = rs.getString("is_nullable");
                    String columnDefault = rs.getString("column_default");

                    StringBuilder colDef = new StringBuilder();
                    colDef.append("    ").append(columnName).append(" ").append(dataType);

                    // Если тип строковый – задаём длину, если задана
                    if (charLength != null &&
                       (dataType.equalsIgnoreCase("character varying") ||
                        dataType.equalsIgnoreCase("varchar") ||
                        dataType.equalsIgnoreCase("character") ||
                        dataType.equalsIgnoreCase("char"))) {
                        colDef.append("(").append(charLength).append(")");
                    }
                    // Для числового типа numeric – задаём точность и масштаб
                    else if (numPrecision != null && dataType.equalsIgnoreCase("numeric")) {
                        colDef.append("(").append(numPrecision);
                        if (numScale != null) {
                            colDef.append(", ").append(numScale);
                        }
                        colDef.append(")");
                    }
                    if ("NO".equalsIgnoreCase(isNullable)) {
                        colDef.append(" NOT NULL");
                    }
                    if (columnDefault != null && !columnDefault.trim().isEmpty()) {
                        colDef.append(" DEFAULT ").append(columnDefault);
                    }
                    columnDefs.add(colDef.toString());
                }
                ddl.append(String.join(",\n", columnDefs));
            }
        }
        ddl.append("\n);");
        return ddl.toString();
    }

    /**
     * Формирует DDL для создания ограничений (аналог ddlx_create_constraints)
     * Обрабатываются: PRIMARY KEY, UNIQUE и FOREIGN KEY.
     */
    public String ddlxCreateConstraints(String schema, String table) throws SQLException {
        StringBuilder ddl = new StringBuilder();

        // Обработка PRIMARY KEY
        String pkSql = "SELECT tc.constraint_name, kcu.column_name " +
                       "FROM information_schema.table_constraints tc " +
                       "JOIN information_schema.key_column_usage kcu " +
                       "  ON tc.constraint_name = kcu.constraint_name " +
                       "WHERE tc.table_schema = ? AND tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY' " +
                       "ORDER BY kcu.ordinal_position";
        try (PreparedStatement stmt = connection.prepareStatement(pkSql)) {
            stmt.setString(1, schema);
            stmt.setString(2, table);
            try (ResultSet rs = stmt.executeQuery()) {
                Map<String, List<String>> pkConstraints = new HashMap<>();
                while (rs.next()) {
                    String constraintName = rs.getString("constraint_name");
                    String columnName = rs.getString("column_name");
                    pkConstraints.computeIfAbsent(constraintName, k -> new ArrayList<>()).add(columnName);
                }
                for (Map.Entry<String, List<String>> entry : pkConstraints.entrySet()) {
                    ddl.append("ALTER TABLE ").append(schema).append(".").append(table)
                        .append(" ADD CONSTRAINT ").append(entry.getKey())
                        .append(" PRIMARY KEY (")
                        .append(String.join(", ", entry.getValue()))
                        .append(");\n");
                }
            }
        }

        // Обработка UNIQUE ограничений
        String uniqueSql = "SELECT tc.constraint_name, kcu.column_name " +
                           "FROM information_schema.table_constraints tc " +
                           "JOIN information_schema.key_column_usage kcu " +
                           "  ON tc.constraint_name = kcu.constraint_name " +
                           "WHERE tc.table_schema = ? AND tc.table_name = ? AND tc.constraint_type = 'UNIQUE' " +
                           "ORDER BY kcu.ordinal_position";
        try (PreparedStatement stmt = connection.prepareStatement(uniqueSql)) {
            stmt.setString(1, schema);
            stmt.setString(2, table);
            try (ResultSet rs = stmt.executeQuery()) {
                Map<String, List<String>> uniqueConstraints = new HashMap<>();
                while (rs.next()) {
                    String constraintName = rs.getString("constraint_name");
                    String columnName = rs.getString("column_name");
                    uniqueConstraints.computeIfAbsent(constraintName, k -> new ArrayList<>()).add(columnName);
                }
                for (Map.Entry<String, List<String>> entry : uniqueConstraints.entrySet()) {
                    ddl.append("ALTER TABLE ").append(schema).append(".").append(table)
                        .append(" ADD CONSTRAINT ").append(entry.getKey())
                        .append(" UNIQUE (")
                        .append(String.join(", ", entry.getValue()))
                        .append(");\n");
                }
            }
        }

        // Обработка FOREIGN KEY ограничений
        String fkSql = "SELECT tc.constraint_name, kcu.column_name, " +
                       "       ccu.table_schema AS foreign_table_schema, " +
                       "       ccu.table_name AS foreign_table_name, " +
                       "       ccu.column_name AS foreign_column_name " +
                       "FROM information_schema.table_constraints tc " +
                       "JOIN information_schema.key_column_usage kcu " +
                       "  ON tc.constraint_name = kcu.constraint_name " +
                       "JOIN information_schema.constraint_column_usage ccu " +
                       "  ON ccu.constraint_name = tc.constraint_name " +
                       "WHERE tc.table_schema = ? AND tc.table_name = ? AND tc.constraint_type = 'FOREIGN KEY' " +
                       "ORDER BY kcu.ordinal_position";
        try (PreparedStatement stmt = connection.prepareStatement(fkSql)) {
            stmt.setString(1, schema);
            stmt.setString(2, table);
            try (ResultSet rs = stmt.executeQuery()) {
                // Группировка по имени ограничения
                Map<String, List<String>> fkColumns = new HashMap<>();
                Map<String, List<String>> refColumns = new HashMap<>();
                Map<String, String> refTables = new HashMap<>();
                while (rs.next()) {
                    String constraintName = rs.getString("constraint_name");
                    String columnName = rs.getString("column_name");
                    String foreignTableSchema = rs.getString("foreign_table_schema");
                    String foreignTableName = rs.getString("foreign_table_name");
                    String foreignColumnName = rs.getString("foreign_column_name");

                    fkColumns.computeIfAbsent(constraintName, k -> new ArrayList<>()).add(columnName);
                    refColumns.computeIfAbsent(constraintName, k -> new ArrayList<>()).add(foreignColumnName);
                    refTables.put(constraintName, foreignTableSchema + "." + foreignTableName);
                }
                for (String constraintName : fkColumns.keySet()) {
                    ddl.append("ALTER TABLE ").append(schema).append(".").append(table)
                        .append(" ADD CONSTRAINT ").append(constraintName)
                        .append(" FOREIGN KEY (")
                        .append(String.join(", ", fkColumns.get(constraintName)))
                        .append(") REFERENCES ").append(refTables.get(constraintName))
                        .append(" (")
                        .append(String.join(", ", refColumns.get(constraintName)))
                        .append(");\n");
                }
            }
        }
        return ddl.toString();
    }

    /**
     * Формирует DDL для установки значений по умолчанию для колонок
     * (аналог ddlx_alter_table_defaults)
     */
    public String ddlxAlterTableDefaults(String schema, String table) throws SQLException {
        StringBuilder ddl = new StringBuilder();
        String sql = "SELECT column_name, column_default " +
                     "FROM information_schema.columns " +
                     "WHERE table_schema = ? AND table_name = ? AND column_default IS NOT NULL";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, table);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String columnDefault = rs.getString("column_default");
                    ddl.append("ALTER TABLE ").append(schema).append(".").append(table)
                       .append(" ALTER COLUMN ").append(columnName)
                       .append(" SET DEFAULT ").append(columnDefault)
                       .append(";\n");
                }
            }
        }
        return ddl.toString();
    }

    /**
     * Формирует DDL для смены владельца таблицы (аналог ddlx_alter_own)
     */
    public String ddlxAlterOwn(String schema, String table) throws SQLException {
        // Получаем имя владельца с помощью функции pg_get_userbyid
        String sql = "SELECT pg_get_userbyid(relowner) AS owner " +
                     "FROM pg_class " +
                     "WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = ?) " +
                     "  AND relname = ?";
        String owner = null;
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, table);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    owner = rs.getString("owner");
                }
            }
        }
        if (owner != null && !owner.isEmpty()) {
            return "ALTER TABLE " + schema + "." + table + " OWNER TO " + owner + ";";
        } else {
            return "";
        }
    }
}
