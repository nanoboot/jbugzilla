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

package org.nanoboot.jbugzilla.persistence.api;

import java.util.List;
import org.nanoboot.jbugzilla.entity.Variant;

/**
 *
 * @author robertvokac
 */
public interface VariantRepo {
    List<Variant> list(int pageNumber,int pageSize, Integer number);
        
    int create(Variant variant);
    Variant read(Integer number);
    void update(Variant variant);
    default void delete(Integer Number) {
        throw new UnsupportedOperationException();
    }
    
}
