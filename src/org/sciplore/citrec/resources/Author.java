/*
	CITREC - Evaluation Framework
    Copyright (C) 2015 SciPlore <team@sciplore.org>
    Copyright (C) 2015 Mario Lipinski <lipinski@sciplore.org>
    Copyright (C) 2015 Norman Meuschke <meuschke@sciplore.org>

    This program is free software; you can redistribute it and/or
    modify it under the terms of the GNU General Public License
    as published by the Free Software Foundation; either version 2
    of the License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/

package org.sciplore.citrec.resources;

/**
 * Resource class for authors.
 * 
 * @author Mario Lipinski <a href="mailto:lipinski@sciplore.org">lipinski@sciplore.org</a>
 * @author Norman Meuschke <a href="mailto:meuschke@sciplore.org">meuschke@sciplore.org</a>
 */
public class Author {
	public int pmcId;
	public String firstName = null;
	public String lastName = null;

	public Author(int pmc, String fN, String lN) {
		this.pmcId = pmc;
		this.firstName = fN;
		this.lastName = lN;
	}

	public Author() {
	}

}
