package edu.umich.verdict.cli;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Vector;


public class ResultWriter {
    static final int
            MAX_WIDTH = 40,
            MIN_WIDTH = 3,
            MAX_RETRIES = 5;

    public static void writeResultSet(PrintWriter out, ResultSet rs)
            throws SQLException {

        ResultSetMetaData rsmd = rs.getMetaData();

        Vector<ResultSet> nestedResults;
        int numberOfRowsSelected = 0;

        int[] displayColumnWidths = getColumnDisplayWidths(rsmd);

        int len = writeHeader(out, rsmd, displayColumnWidths);

        // When displaying rows, keep going past errors
        // unless/until the maximum # of errors is reached.
        int retry = 0;

        boolean doNext = true;
        while (doNext) {
            try {
                doNext = rs.next();
                if (doNext) {

                    writeRow(out, rs, rsmd, len, displayColumnWidths);
                    ShowWarnings(out, rs);
                    numberOfRowsSelected++;
                }
            } catch (SQLException e) {
                // REVISIT: might want to check the exception
                // and for some, not bother with the retry.
                if (++retry > MAX_RETRIES)
                    throw e;
                else
                    ShowSQLException(out, e);
            }
        }
    }

    static public void ShowWarnings(PrintWriter out, SQLWarning warning) {
        while (warning != null) {
            String p1 = warning.getSQLState();
            String p2 = warning.getMessage();
            out.println("Warning: "+p2+"\n\t SQLState: "+p1);
            warning = warning.getNextWarning();
        }
    }

    static public void ShowWarnings(PrintWriter out, ResultSet rs) {
        try {
            if (rs != null) {
                ShowWarnings(out, rs.getWarnings());
            }

            if (rs != null) {
                rs.clearWarnings();
            }
        } catch (SQLException e) {
            ShowSQLException(out, e);
        }
    }

    private static void ShowSQLException(PrintWriter out, SQLException e) {
        String errorCode;

            errorCode = " (errorCode = " + e.getErrorCode() + ")";

        while (e!=null) {
            out.println("ERROR "+mapNull(e.getSQLState(),"(no SQLState)")+": "+
                    mapNull(e.getMessage(),"(no message)")+errorCode);
            e.printStackTrace(out);
            out.flush();
            e=e.getNextException();
        }
    }

    static public String mapNull(String s, String nullValue) {
        if (s==null) return nullValue;
        return s;
    }

    private static void writeRow(PrintWriter out, ResultSet rs, ResultSetMetaData rsmd, int rowLen, int[] displayColumnWidths) throws SQLException {
        StringBuffer buf = new StringBuffer();
        buf.ensureCapacity(rowLen);

        int numCols = displayColumnWidths.length;
        int i;

        // get column header info
        // truncate it to the column display width
        // add a bar between each item.
        for (i=1; i <= numCols; i++){
            int colnum = i;
            if (i>1)
                buf.append('|');

            String s;
            switch (rsmd.getColumnType(colnum)) {
                default:
                    s = rs.getString(colnum);
                    break;
                case Types.JAVA_OBJECT:
                case Types.OTHER:
                {
                    Object o = rs.getObject(colnum);
                    if (o == null) { s = "NULL"; }
                    else
                    {
                        try {
                            s = rs.getString(colnum);
                        } catch (SQLException se) {
                            // oops, they don't support refetching the column
                            s = o.toString();
                        }
                    }
                }
                break;
            }

            if (s==null) s = "NULL";

            int w = displayColumnWidths[i-1];
            if (s.length() < w) {
                StringBuilder fullS = new StringBuilder(s);
                fullS.ensureCapacity(w);
                for (int k=s.length(); k<w; k++)
                    fullS.append(' ');
                s = fullS.toString();
            }
            else if (s.length() > w)
                // add the & marker to know it got cut off
                s = s.substring(0,w-1)+"&";

            buf.append(s);
        }
        out.println(buf);
    }

    private static int writeHeader(PrintWriter out, ResultSetMetaData rsmd, int[] displayColumnWidths) throws SQLException {
        StringBuffer buf = new StringBuffer();

        int numCols = displayColumnWidths.length;
        int rowLen;

        // do some precalculation so the buffer is allocated only once
        // buffer is twice as long as the display length plus one for a newline
        rowLen = (numCols - 1); // for the column separators
        for (int i = 1; i <= numCols; i++)
            rowLen += displayColumnWidths[i - 1];
        buf.ensureCapacity(rowLen);

        // get column header info
        // truncate it to the column display width
        // add a bar between each item.
        for (int i = 1; i <= numCols; i++) {
            int colnum = i;

            if (i > 1)
                buf.append('|');

            String s = rsmd.getColumnLabel(colnum);

            int w = displayColumnWidths[i - 1];

            if (s.length() < w) {

                buf.append(s);

                // try to paste on big chunks of space at a time.
                int k = w - s.length();
                for (; k >= 64; k -= 64)
                    buf.append(
                            "                                                                ");
                for (; k >= 16; k -= 16)
                    buf.append("                ");
                for (; k >= 4; k -= 4)
                    buf.append("    ");
                for (; k > 0; k--)
                    buf.append(' ');
            } else if (s.length() > w) {
                if (w > 1)
                    buf.append(s.substring(0, w - 1));
                if (w > 0)
                    buf.append('&');
            } else {
                buf.append(s);
            }
        }

        buf.setLength(Math.min(rowLen, 1024));
        out.println(buf);

        // now print a row of '-'s
        for (int i = 0; i < Math.min(rowLen, 1024); i++)
            buf.setCharAt(i, '-');
        out.println(buf);

        return rowLen;
    }

    static private int[] getColumnDisplayWidths(ResultSetMetaData rsmd)
            throws SQLException {
        int count = rsmd.getColumnCount();
        int[] widths = new int[count];

        for (int i = 0; i < count; i++) {
            int colnum = (i + 1);
            int dispsize = rsmd.getColumnDisplaySize(colnum);
            widths[i] = Math.min(MAX_WIDTH,
                    Math.max((rsmd.isNullable(colnum) == ResultSetMetaData.columnNoNulls) ?
                            0 : MIN_WIDTH, dispsize));
        }
        return widths;
    }

}
