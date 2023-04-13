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

/**
 *
 * @author robertvokac
 */
public class VariantTable {
    public static final String TABLE_NAME = "VARIANT";
    
    public static final String NUMBER = "NUMBER";
    public static final String NAME = "NAME";
    public static final String NOTE = "NOTE";
    public static final String STATUS = "STATUS";
    public static final String AUTHOR = "AUTHOR";
    
    public static final String LICENCE = "LICENCE";
    public static final String OPEN_SOURCE = "OPEN_SOURCE";
    public static final String USER_INTERFACE = "USER_INTERFACE";
    public static final String PROGRAMMING_LANGUAGE = "PROGRAMMING_LANGUAGE";
    public static final String BINARIES = "BINARIES";
    
    public static final String LAST_UPDATE = "LAST_UPDATE";
    public static final String LAST_VERSION = "LAST_VERSION";
    
    
    private VariantTable() {
        //Not meant to be instantiated.
    }
    
}
