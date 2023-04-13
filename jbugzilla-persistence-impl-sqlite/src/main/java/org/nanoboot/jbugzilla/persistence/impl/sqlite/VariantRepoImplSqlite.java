///////////////////////////////////////////////////////////////////////////////////////////////
// JBugzilla.
// Copyright (C) 2023-2023 the original author or authors.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; version 2
// of the License only.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
///////////////////////////////////////////////////////////////////////////////////////////////
package org.nanoboot.jbugzilla.persistence.impl.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Setter;
import org.nanoboot.jbugzilla.entity.Variant;
import org.nanoboot.jbugzilla.persistence.api.VariantRepo;
import org.nanoboot.powerframework.time.moment.LocalDate;

/**
 *
 * @author robertvokac
 */
public class VariantRepoImplSqlite implements VariantRepo {

    @Setter
    private SqliteConnectionFactory sqliteConnectionFactory;

    @Override
    public List<Variant> list(int pageNumber, int pageSize, Integer number) {
        int numberEnd = pageSize * pageNumber;
        int numberStart = numberEnd - pageSize + 1;

        List<Variant> result = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb
                .append("SELECT * FROM ")
                .append(VariantTable.TABLE_NAME)
                .append(" WHERE ");
        boolean pagingIsEnabled = number == null;

        if (pagingIsEnabled) {
            sb.append(VariantTable.NUMBER)
                    .append(" BETWEEN ? AND ? ");
        } else {
            sb.append("1=1");
        }

        if (number != null) {
            sb.append(" AND ").append(WebsiteTable.NUMBER)
                    .append("=?");
        }

        String sql = sb.toString();
        System.err.println(sql);
        int i = 0;
        ResultSet rs = null;
        try (
                 Connection connection = sqliteConnectionFactory.createConnection();  PreparedStatement stmt = connection.prepareStatement(sql);) {
            if (pagingIsEnabled) {
                stmt.setInt(++i, numberStart);
                stmt.setInt(++i, numberEnd);
            }

            if (number != null) {
                stmt.setInt(++i, number);
            }
            System.err.println(stmt.toString());
            rs = stmt.executeQuery();

            while (rs.next()) {
                result.add(extractVariantFromResultSet(rs));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(VariantRepoImplSqlite.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException ex) {
                Logger.getLogger(VariantRepoImplSqlite.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return result;
    }

    private static Variant extractVariantFromResultSet(final ResultSet rs) throws SQLException {
        String lastUpdateTmp = rs.getString(VariantTable.LAST_UPDATE);
        return new Variant(
                rs.getInt(VariantTable.NUMBER),
                rs.getString(VariantTable.NAME),
                rs.getString(VariantTable.NOTE),
                rs.getString(VariantTable.STATUS),
                rs.getString(VariantTable.AUTHOR),
                //
                rs.getString(VariantTable.LICENCE),
                rs.getInt(VariantTable.OPEN_SOURCE) != 0,
                rs.getString(VariantTable.USER_INTERFACE),
                rs.getString(VariantTable.PROGRAMMING_LANGUAGE),
                rs.getInt(VariantTable.BINARIES) != 0,
                //

                lastUpdateTmp == null ? null : new LocalDate(lastUpdateTmp),
                rs.getString(VariantTable.LAST_VERSION)
        );
        }

        @Override
        public int create
        (Variant variant
        
        
            ) {
        StringBuilder sb = new StringBuilder();
            sb
                    .append("INSERT INTO ")
                    .append(VariantTable.TABLE_NAME)
                    .append("(")
                    .append(VariantTable.NAME).append(",")
                    .append(VariantTable.NOTE).append(",")
                    .append(VariantTable.STATUS).append(",")
                    .append(VariantTable.AUTHOR).append(",")
                    //
                    .append(VariantTable.LICENCE).append(",")
                    .append(VariantTable.OPEN_SOURCE).append(",")
                    .append(VariantTable.USER_INTERFACE).append(",")
                    .append(VariantTable.PROGRAMMING_LANGUAGE).append(",")
                    .append(VariantTable.BINARIES).append(",")
                    //
                    .append(VariantTable.LAST_UPDATE).append(",")
                    .append(VariantTable.LAST_VERSION);
            //

            sb.append(")")
                    .append(" VALUES (?,?,?,? ,?,?,?,?,? ,?,?)");

            String sql = sb.toString();
            System.err.println(sql);
            try (
                     Connection connection = sqliteConnectionFactory.createConnection();  PreparedStatement stmt = connection.prepareStatement(sql);) {
                int i = 0;
                stmt.setString(++i, variant.getName());
                stmt.setString(++i, variant.getNote());
                stmt.setString(++i, variant.getStatus());
                stmt.setString(++i, variant.getAuthor());
                //
                stmt.setString(++i, variant.getLicence());
                stmt.setInt(++i, variant.getOpenSource() ? 1 : 0);
                stmt.setString(++i, variant.getUserInterface());
                stmt.setString(++i, variant.getProgrammingLanguage());
                stmt.setInt(++i, variant.getBinaries() ? 1 : 0);
                //
                stmt.setString(++i, variant.getLastUpdate() == null ? null : variant.getLastUpdate().toString());
                stmt.setString(++i, variant.getLastVersion());
                //

                stmt.execute();
                System.out.println(stmt.toString());
                ResultSet rs = connection.createStatement().executeQuery("select last_insert_rowid() as last");
                while (rs.next()) {
                    int numberOfNewVariant = rs.getInt("last");
                    System.out.println("numberOfNewVariant=" + numberOfNewVariant);
                    return numberOfNewVariant;
                }

            } catch (SQLException e) {
                System.out.println(e.getMessage());
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(VariantRepoImplSqlite.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.err.println("Error.");
            return 0;
        }

        @Override
        public Variant read
        (Integer number
        
        
            ) {

        if (number == null) {
                throw new RuntimeException("number is null");
            }
            StringBuilder sb = new StringBuilder();
            sb
                    .append("SELECT * FROM ")
                    .append(VariantTable.TABLE_NAME)
                    .append(" WHERE ")
                    .append(VariantTable.NUMBER)
                    .append("=?");

            String sql = sb.toString();
            int i = 0;
            ResultSet rs = null;
            try (
                     Connection connection = sqliteConnectionFactory.createConnection();  PreparedStatement stmt = connection.prepareStatement(sql);) {

                stmt.setInt(++i, number);

                rs = stmt.executeQuery();

                while (rs.next()) {
                    return extractVariantFromResultSet(rs);
                }
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(VariantRepoImplSqlite.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                } catch (SQLException ex) {
                    Logger.getLogger(VariantRepoImplSqlite.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            return null;
        }

        @Override
        public void update
        (Variant variant
        
        
            ) {
        StringBuilder sb = new StringBuilder();
            sb
                    .append("UPDATE ")
                    .append(VariantTable.TABLE_NAME)
                    .append(" SET ")
                    .append(VariantTable.NAME).append("=?, ")
                    .append(VariantTable.NOTE).append("=?, ")
                    .append(VariantTable.STATUS).append("=?, ")
                    .append(VariantTable.AUTHOR).append("=?, ")
                    //
                    .append(VariantTable.LICENCE).append("=?, ")
                    .append(VariantTable.OPEN_SOURCE).append("=?, ")
                    .append(VariantTable.USER_INTERFACE).append("=?, ")
                    .append(VariantTable.PROGRAMMING_LANGUAGE).append("=?, ")
                    .append(VariantTable.BINARIES).append("=?, ")
                    //
                    .append(VariantTable.LAST_UPDATE).append("=?, ")
                    .append(VariantTable.LAST_VERSION).append("=? ")
                    .append(" WHERE ").append(VariantTable.NUMBER).append("=?");

            String sql = sb.toString();
            System.err.println(sql);
            try (
                     Connection connection = sqliteConnectionFactory.createConnection();  PreparedStatement stmt = connection.prepareStatement(sql);) {
                int i = 0;
                stmt.setString(++i, variant.getName());
                stmt.setString(++i, variant.getNote());
                stmt.setString(++i, variant.getStatus());
                stmt.setString(++i, variant.getAuthor());
                //
                //
                stmt.setString(++i, variant.getLicence());
                if (variant.getOpenSource() == null) {
                    stmt.setNull(++i, java.sql.Types.INTEGER);
                } else {
                    stmt.setInt(++i, variant.getOpenSource() ? 1 : 0);
                }
                stmt.setString(++i, variant.getUserInterface());
                stmt.setString(++i, variant.getProgrammingLanguage());

                if (variant.getBinaries() == null) {
                    stmt.setNull(++i, java.sql.Types.INTEGER);
                } else {
                    stmt.setInt(++i, variant.getBinaries() ? 1 : 0);
                }

                //
                if (variant.getLastUpdate() == null) {
                    stmt.setNull(++i, java.sql.Types.VARCHAR);
                } else {
                    stmt.setString(++i, variant.getLastUpdate().toString());
                }
                stmt.setString(++i, variant.getLastVersion());
                //
                stmt.setInt(++i, variant.getNumber());

                int numberOfUpdatedRows = stmt.executeUpdate();
                System.out.println("numberOfUpdatedRows=" + numberOfUpdatedRows);
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(VariantRepoImplSqlite.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }
