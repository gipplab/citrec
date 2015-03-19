/*
	CITREC - Evaluation Framework
    Copyright (C) 2015 SciPlore <team@sciplore.org>
    Copyright (C) 2015 Matt Walters <team@sciplore.org>

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

package org.sciplore.citrec.dataimport;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 *
 * @author Matt Walters <a href="mailto:team@sciplore.org">team@sciplore.org</a>
 */
public class TrecAuditor {
    
    
    private static BufferedWriter writer;
    
    
    public synchronized static void setStaticWriterOutput(File f){
        try {
            writer = new BufferedWriter(new FileWriter(f));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
    
    public synchronized static void closeStaticWriter(){
        try {
            writer.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static String write(String message, File file) throws IOException {
        assert file != null;
        String addition = message + "\n" + file.getAbsolutePath() + "\n";
        writer.write(addition);
        return addition;
        
    }
   
}
