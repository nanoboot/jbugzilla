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
package org.nanoboot.jbugzilla.web.misc.utils;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;

/**
 *
 * @author <a href="mailto:robertvokac@nanoboot.org">Robert Vokac</a>
 * @since 0.0.0
 */
public class Utils {

    public static String getBaseUrl(HttpServletRequest request) {
        return request.getServerName() + ':' + request.getServerPort() + request.getContextPath() + '/';
    }

    public static String formatToHtmlWithoutEmptyWord(Object o) {
        return formatToHtml(o, false);
    }

    public static String formatToHtml(Object o) {
        return formatToHtml(o, true);
    }

    public static String formatToHtml(Object o, boolean withEmptyWord) {
        if (o == null) {
            return withEmptyWord ? "[empty]" : "";
        }
        if (o instanceof String && (((String) o)).isEmpty()) {
            return withEmptyWord ? "[empty]" : "";
        }
        if (o instanceof Boolean) {
            Boolean b = (Boolean) o;
            return b.booleanValue() ? "<span style=\"color:#00CC00;font-weight:bold;\">YES</span>" : "<span style=\"color:red;font-weight:bold;\">NO</span>";
        }
        return o.toString();
    }


    public static boolean cannotUpdate(HttpServletRequest request) {
        return !canUpdate(request);
    }
    public static boolean canUpdate(HttpServletRequest request) {
        //if(true)return true;

        String allcanupdate = System.getProperty("jbugzilla.allcanupdate");
        if(allcanupdate != null && allcanupdate.equals("true")) {
            return true;
        }
        HttpSession session = request.getSession(false);
        if (session == null) {
            return false;
        }
        Object canUpdateAttribute = session.getAttribute("canUpdate");
        if (canUpdateAttribute == null) {
            return false;
        }
        return canUpdateAttribute.equals("true");

    }
}
