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
package org.nanoboot.jbugzilla.web.filters;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

public class AuthenticationFilter implements Filter {

    private ServletContext context;

    public void init(FilterConfig fConfig) throws ServletException {
        this.context = fConfig.getServletContext();
        this.context.log("AuthenticationFilter started");
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {

        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse res = (HttpServletResponse) response;

        HttpSession session = req.getSession(false);
        boolean sessionExists = session != null;

        String octConfpath = System.getProperty("jbugzilla.confpath");
        if (octConfpath == null || octConfpath.isEmpty()) {
            String msg = "csa configuration is broken : " + "jbugzilla.confpath=" + octConfpath;
            throw new RuntimeException(msg);
        }
        File octagonProperties = new File(octConfpath + "/jbugzilla.properties");
        try ( InputStream input = new FileInputStream(octagonProperties.getAbsolutePath())) {

            Properties properties = new Properties();
            properties.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException("Loading jbugzilla.properties failed.");
        }

        if (!sessionExists) {
            res.sendRedirect(req.getContextPath() + "/login.html");
            this.context.log("Access is not authorized.");

        } else {
            this.context.log("Access is authorized.");
            chain.doFilter(request, response);
        }
    }

    public void destroy() {
        //close any resources here
    }
}
