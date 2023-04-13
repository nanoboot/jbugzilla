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
package org.nanoboot.jbugzilla.persistence.impl.mock;

import java.util.ArrayList;
import java.util.List;
import org.nanoboot.jbugzilla.entity.Variant;
import org.nanoboot.jbugzilla.persistence.api.VariantRepo;

/**
 *
 * @author robertvokac
 */
public class VariantRepoImplMock implements VariantRepo {

    private final List<Variant> internalList = new ArrayList<>();

    private int nextNumber = 1;

    @Override
    public List<Variant> list(int pageNumber, int pageSize, Integer number) {
        if (internalList.isEmpty()) {
            for(int i = 0;i< 50;i++) {
            internalList.add(
                    new Variant(
                            nextNumber++,
                            "name",
                            "image",
                            "status",
                            "author",
                            "licence",
                            true,
                            "userInterface",
                            "Java",
                            false,
                    null, "0.1.4"));
            }
        }
        List<Variant> finalList = new ArrayList<>();
        
        int numberEnd = pageSize * pageNumber;
        int numberStart = numberEnd - pageSize + 1;
        for (Variant v : internalList) {
            if(number != null) {
                if(v.getNumber().intValue() == number.intValue()) {
                    finalList.add(v);
                    break;
                } else {
                    continue;
                }
            }
            if (v.getNumber() < numberStart || v.getNumber() > numberEnd) {
                continue;
            }
            
            finalList.add(v);

        }
        return finalList;
    
    }

    @Override
    public int create(Variant variant) {
        variant.setNumber(nextNumber++);
        internalList.add(variant);
        return variant.getNumber();
    }

    @Override
    public Variant read(Integer number) {
        for (Variant w : internalList) {
            if (w.getNumber().intValue() == number.intValue()) {
                return w;
            }
        }
        return null;
    }

    @Override
    public void update(Variant variant) {
        Variant variantToBeDeleted = null;
        for (Variant v : internalList) {
            if (v.getNumber().intValue() == variant.getNumber().intValue()) {
                variantToBeDeleted = v;
                break;
            }
        }
        if (variantToBeDeleted == null) {
            //nothing to do
            return;
        }
        internalList.remove(variantToBeDeleted);
        internalList.add(variant);

    }

}
