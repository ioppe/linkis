package org.apache.linkis.metadata.query.service.druid;

import org.apache.commons.collections.MapUtils;
import org.apache.linkis.common.conf.CommonVars;
import org.apache.linkis.metadata.query.common.domain.MetaColumnInfo;
import org.apache.linkis.metadata.query.service.AbstractSqlConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlConnection extends AbstractSqlConnection {

    private static final Logger LOG = LoggerFactory.getLogger(org.apache.linkis.metadata.query.service.druid.SqlConnection.class);

    private static final CommonVars<String> SQL_DRIVER_CLASS = CommonVars.apply("wds.linkis.server.mdm.service.druid.driver", "org.apache.calcite.avatica.remote.Driver");
    private static final CommonVars<String> SQL_CONNECT_URL = CommonVars.apply("wds.linkis.server.mdm.service.druid.url", "jdbc:avatica:remote:url=http://%s:%s/druid/v2/sql/avatica/");

    public SqlConnection(String host, Integer port, String username, String password, String database, Map<String, Object> extraParams) throws ClassNotFoundException, SQLException {
        super(host, port, username, password, database, extraParams);
    }

    public List<String> getAllDatabases() throws SQLException {
        List<String> dataBaseName = new ArrayList<>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT DISTINCT datasource FROM sys.segments");
            while (rs.next()) {
                dataBaseName.add(rs.getString(1));
            }
        } finally {
            closeResource(null, stmt, rs);
        }
        return dataBaseName;
    }

    public List<MetaColumnInfo> getColumns(String database, String table)
            throws SQLException, ClassNotFoundException {
        List<MetaColumnInfo> columns = new ArrayList<>();
        String columnSql = "SELECT * FROM `" + database + "` WHERE 1 = 2";
        PreparedStatement ps = null;
        ResultSet rs = null;
        ResultSetMetaData meta = null;
        try {
            List<String> primaryKeys = getPrimaryKeys(table);
            ps = conn.prepareStatement(columnSql);
            rs = ps.executeQuery();
            meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            for (int i = 1; i < columnCount + 1; i++) {
                MetaColumnInfo info = new MetaColumnInfo();
                info.setIndex(i);
                info.setLength(meta.getColumnDisplaySize(i));
                info.setNullable((meta.isNullable(i) == ResultSetMetaData.columnNullable));
                info.setName(meta.getColumnName(i));
                info.setType(meta.getColumnTypeName(i));
                if (primaryKeys.contains(meta.getColumnName(i))) {
                    info.setPrimaryKey(true);
                }
                columns.add(info);
            }
        } finally {
            closeResource(null, ps, rs);
        }
        return columns;
    }

    public Connection getDBConnection(ConnectMessage connectMessage, String database)
            throws ClassNotFoundException, SQLException {
        Class.forName(SQL_DRIVER_CLASS.getValue());
        String url =
                String.format(
                        SQL_CONNECT_URL.getValue(), connectMessage.host, connectMessage.port);
        // deal with empty database
//        if (StringUtils.isBlank(database)) {
//            url = url.substring(0, url.length() - 1);
//        }
        if (MapUtils.isNotEmpty(connectMessage.extraParams)) {
            String extraParamString =
                    connectMessage.extraParams.entrySet().stream()
                            .map(e -> String.join("=", e.getKey(), String.valueOf(e.getValue())))
                            .collect(Collectors.joining("&"));
            url += "?" + extraParamString;
        }
        LOG.info("jdbc connection url: {}", url);
        return DriverManager.getConnection(url, connectMessage.username, connectMessage.password);
    }

    public String getSqlConnectUrl() {
        return SQL_CONNECT_URL.getValue();
    }

}
