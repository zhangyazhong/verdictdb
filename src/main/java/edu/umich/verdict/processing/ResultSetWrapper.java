package edu.umich.verdict.processing;

import com.cloudera.dsi.dataengine.interfaces.IColumn;
import com.cloudera.dsi.dataengine.utilities.Nullable;
import com.cloudera.dsi.dataengine.utilities.Searchable;
import com.cloudera.dsi.dataengine.utilities.TypeMetadata;
import com.cloudera.dsi.dataengine.utilities.Updatable;
import com.cloudera.jdbc.jdbc41.S41ResultSetMetaData;
import com.cloudera.support.*;
import com.cloudera.support.exceptions.ErrorException;
import edu.umich.verdict.transformation.TransformedQuery;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

//TODO: remove or replace
public class ResultSetWrapper implements ResultSet {
    ResultSet rs;
    TransformedQuery q;
    double[][] intervals;
    double[] errors;

    public ResultSetWrapper(ResultSet rs, TransformedQuery q) {
        this.rs = rs;
        this.q = q;
        intervals = new double[q.getAggregates().size()][2];
        errors = new double[q.getAggregates().size()];
    }

    public boolean next() throws SQLException {
        if (!rs.next())
            return false;
        setIntervals();
        return true;
    }

    private void setIntervals() {
        int base = q.getOriginalCols() + 1, c = q.getBootstrapRepeats(), margin = (int) (c * (1 - q.getConfidence()) / 2);
        for (int j = 0; j < q.getAggregates().size(); j++)
            try {
                TransformedQuery.AggregateInfo aggr = q.getAggregates().get(j);
                double[] nums = new double[c];
                for (int i = 0; i < c; i++)
                    nums[i] = rs.getDouble(i + base);
                base += c;
                Arrays.sort(nums);
                double res = rs.getDouble(aggr.getColumn()), l = nums[margin], r = nums[c - margin - 1];
                intervals[j][0] = l;
                intervals[j][1] = r;
                errors[j] = Math.abs(Math.max(Math.abs(l - res), Math.abs(r - res)) / res) * 100;
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }

    public void close() throws SQLException {
        rs.close();
    }

    public boolean wasNull() throws SQLException {
        return rs.wasNull();
    }

    public String getString(int columnIndex) throws SQLException {
        if (columnIndex <= q.getOriginalCols())
            return rs.getString(columnIndex);
        return getInterval(columnIndex - q.getOriginalCols());
    }

    private String getInterval(int i) {
        if (i % 2 == 1)
            return "[" + intervals[(i - 1) / 2][0] + ", " + intervals[(i - 1) / 2][1] + "]";
        return Math.round(errors[(i - 2) / 2] * 1000) / 1000.0 + "%";
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return false;
    }

    public byte getByte(int columnIndex) throws SQLException {
        return 0;
    }

    public short getShort(int columnIndex) throws SQLException {
        return 0;
    }

    public int getInt(int columnIndex) throws SQLException {
        return 0;
    }

    public long getLong(int columnIndex) throws SQLException {
        return 0;
    }

    public float getFloat(int columnIndex) throws SQLException {
        return 0;
    }

    public double getDouble(int columnIndex) throws SQLException {
        return 0;
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return null;
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        return new byte[0];
    }

    public Date getDate(int columnIndex) throws SQLException {
        return null;
    }

    public Time getTime(int columnIndex) throws SQLException {
        return null;
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return null;
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return null;
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return null;
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return null;
    }

    public String getString(String columnLabel) throws SQLException {
        return null;
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        return false;
    }

    public byte getByte(String columnLabel) throws SQLException {
        return 0;
    }

    public short getShort(String columnLabel) throws SQLException {
        return 0;
    }

    public int getInt(String columnLabel) throws SQLException {
        return 0;
    }

    public long getLong(String columnLabel) throws SQLException {
        return 0;
    }

    public float getFloat(String columnLabel) throws SQLException {
        return 0;
    }

    public double getDouble(String columnLabel) throws SQLException {
        return 0;
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return null;
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        return new byte[0];
    }

    public Date getDate(String columnLabel) throws SQLException {
        return null;
    }

    public Time getTime(String columnLabel) throws SQLException {
        return null;
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return null;
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return null;
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return null;
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return null;
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {

    }

    public String getCursorName() throws SQLException {
        return null;
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        S41ResultSetMetaData meta = (S41ResultSetMetaData) rs.getMetaData();
        List<IColumn> cols = new ArrayList<>();
        for (int i = 1; i <= q.getOriginalCols(); i++) {
            cols.add(new ColumnMetaData(meta.getColumnName(i), meta.getColumnDisplaySize(i), meta.isNullable(i), meta.getColumnType(i)));
        }
        for (TransformedQuery.AggregateInfo aggr : q.getAggregates()) {
            try {
                cols.add(new ColumnMetaData(meta.getColumnName(aggr.getColumn()) + " Conf. Int.", 40, 0, 12));
                cols.add(new ColumnMetaData(meta.getColumnName(aggr.getColumn()) + " Appr. Err.", 20, 0, 12));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return new S41ResultSetMetaData(cols, new VoidLogger(), new VoidWarningListener());
    }

    public Object getObject(int columnIndex) throws SQLException {
        if (columnIndex <= q.getOriginalCols())
            return rs.getObject(columnIndex);
        return getInterval(columnIndex - q.getOriginalCols());
    }

    public Object getObject(String columnLabel) throws SQLException {
        return null;
    }

    public int findColumn(String columnLabel) throws SQLException {
        return 0;
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return null;
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return null;
    }

    public boolean isBeforeFirst() throws SQLException {
        return false;
    }

    public boolean isAfterLast() throws SQLException {
        return false;
    }

    public boolean isFirst() throws SQLException {
        return false;
    }

    public boolean isLast() throws SQLException {
        return false;
    }

    public void beforeFirst() throws SQLException {

    }

    public void afterLast() throws SQLException {

    }

    public boolean first() throws SQLException {
        return false;
    }

    public boolean last() throws SQLException {
        return false;
    }

    public int getRow() throws SQLException {
        return 0;
    }

    public boolean absolute(int row) throws SQLException {
        return false;
    }

    public boolean relative(int rows) throws SQLException {
        return false;
    }

    public boolean previous() throws SQLException {
        return false;
    }

    public void setFetchDirection(int direction) throws SQLException {

    }

    public int getFetchDirection() throws SQLException {
        return 0;
    }

    public void setFetchSize(int rows) throws SQLException {

    }

    public int getFetchSize() throws SQLException {
        return 0;
    }

    public int getType() throws SQLException {
        return 0;
    }

    public int getConcurrency() throws SQLException {
        return 0;
    }

    public boolean rowUpdated() throws SQLException {
        return false;
    }

    public boolean rowInserted() throws SQLException {
        return false;
    }

    public boolean rowDeleted() throws SQLException {
        return false;
    }

    public void updateNull(int columnIndex) throws SQLException {

    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {

    }

    public void updateByte(int columnIndex, byte x) throws SQLException {

    }

    public void updateShort(int columnIndex, short x) throws SQLException {

    }

    public void updateInt(int columnIndex, int x) throws SQLException {

    }

    public void updateLong(int columnIndex, long x) throws SQLException {

    }

    public void updateFloat(int columnIndex, float x) throws SQLException {

    }

    public void updateDouble(int columnIndex, double x) throws SQLException {

    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

    }

    public void updateString(int columnIndex, String x) throws SQLException {

    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {

    }

    public void updateDate(int columnIndex, Date x) throws SQLException {

    }

    public void updateTime(int columnIndex, Time x) throws SQLException {

    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

    }

    public void updateObject(int columnIndex, Object x) throws SQLException {

    }

    public void updateNull(String columnLabel) throws SQLException {

    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {

    }

    public void updateByte(String columnLabel, byte x) throws SQLException {

    }

    public void updateShort(String columnLabel, short x) throws SQLException {

    }

    public void updateInt(String columnLabel, int x) throws SQLException {

    }

    public void updateLong(String columnLabel, long x) throws SQLException {

    }

    public void updateFloat(String columnLabel, float x) throws SQLException {

    }

    public void updateDouble(String columnLabel, double x) throws SQLException {

    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

    }

    public void updateString(String columnLabel, String x) throws SQLException {

    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {

    }

    public void updateDate(String columnLabel, Date x) throws SQLException {

    }

    public void updateTime(String columnLabel, Time x) throws SQLException {

    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

    }

    public void updateObject(String columnLabel, Object x) throws SQLException {

    }

    public void insertRow() throws SQLException {

    }

    public void updateRow() throws SQLException {

    }

    public void deleteRow() throws SQLException {

    }

    public void refreshRow() throws SQLException {

    }

    public void cancelRowUpdates() throws SQLException {

    }

    public void moveToInsertRow() throws SQLException {

    }

    public void moveToCurrentRow() throws SQLException {

    }

    public Statement getStatement() throws SQLException {
        return null;
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    public Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    public Clob getClob(int columnIndex) throws SQLException {
        return null;
    }

    public Array getArray(int columnIndex) throws SQLException {
        return null;
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    public Clob getClob(String columnLabel) throws SQLException {
        return null;
    }

    public Array getArray(String columnLabel) throws SQLException {
        return null;
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    public int getHoldability() throws SQLException {
        return 0;
    }

    public boolean isClosed() throws SQLException {
        return false;
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    public NClob getNClob(int columnIndex) throws SQLException {
        return null;
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        return null;
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return null;
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return null;
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    public String getNString(int columnIndex) throws SQLException {
        return null;
    }

    public String getNString(String columnLabel) throws SQLException {
        return null;
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return null;
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}

class ColumnMetaData implements IColumn {
    String name;
    int length;
    private Nullable nullable;
    private TypeMetadata type;

    public ColumnMetaData(String name, int length, int nullable, int type) {
        this.name = name;
        this.length = length;
        this.nullable = nullable == 1 ? Nullable.NULLABLE : Nullable.NO_NULLS;
        try {
            this.type = TypeMetadata.createTypeMetadata(type);
        } catch (ErrorException e) {
            e.printStackTrace();
        }
    }

    public String getCatalogName() {
        return null;
    }

    public long getColumnLength() {
        return length;
    }

    public long getDisplaySize() throws ErrorException {
        return length;
    }

    public String getLabel() {
        return name;
    }

    public String getName() {
        return name;
    }

    public Nullable getNullable() {
        return nullable;
    }

    public String getSchemaName() {
        return null;
    }

    public Searchable getSearchable() {
        return null;
    }

    public String getTableName() {
        return null;
    }

    public TypeMetadata getTypeMetadata() {
        return type;
    }

    public Updatable getUpdatable() {
        return null;
    }

    public boolean isAutoUnique() {
        return false;
    }

    public boolean isCaseSensitive() {
        return false;
    }

    public boolean isDefinitelyWritable() {
        return false;
    }

    public boolean isUnnamed() {
        return false;
    }
}

class VoidLogger implements ILogger {

    public Locale getLocale() {
        return null;
    }

    public LogLevel getLogLevel() {
        return null;
    }

    public boolean isEnabled() {
        return false;
    }

    public void logDebug(String s, String s1, String s2, ErrorException e) {

    }

    public void logDebug(String s, String s1, String s2, String s3) {

    }

    public void logError(String s, String s1, String s2, ErrorException e) {

    }

    public void logError(String s, String s1, String s2, String s3) {

    }

    public void logFatal(String s, String s1, String s2, String s3) {

    }

    public void logFunctionEntrance(String s, String s1, String s2) {

    }

    public void logInfo(String s, String s1, String s2, String s3) {

    }

    public void logTrace(String s, String s1, String s2, String s3) {

    }

    public void logWarning(String s, String s1, String s2, String s3) {

    }

    public void setLocale(Locale locale) {

    }

    public void setLogLevel(LogLevel logLevel) {

    }
}

class VoidWarningListener implements IWarningListener {

    public Locale getLocale() {
        return null;
    }

    public IMessageSource getMessageSource() {
        return null;
    }

    public void postWarning(Warning warning) {

    }

    public List<Warning> getWarnings() {
        return null;
    }

    public void setLocale(Locale locale) {

    }
}