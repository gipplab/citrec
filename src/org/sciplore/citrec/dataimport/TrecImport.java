/*
	CITREC - Evaluation Framework
    Copyright (C) 2015 SciPlore <team@sciplore.org>
    Copyright (C) 2015 Matt Walters <team@sciplore.org>
    Copyright (C) 2015 Mario Lipinski <lipinski@sciplore.org>

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

import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.sciplore.citrec.Helper;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;

/**
 *
 * @author Matt Walters <a href="mailto:team@sciplore.org">team@sciplore.org</a>
 */
public class TrecImport {

    private final static int NUM_THREADS = 8;
    private static BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(NUM_THREADS * 2);
    private static ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
            NUM_THREADS, NUM_THREADS, 30, TimeUnit.SECONDS, workQueue, new ThreadPoolExecutor.CallerRunsPolicy());
    private static ComboPooledDataSource cpds;

    public static void main(String[] args) throws Exception {
        Properties p = Helper.getProperties();

        cpds = new ComboPooledDataSource();
        cpds.setMinPoolSize(1);
        cpds.setAcquireIncrement(1);
        cpds.setMaxPoolSize(NUM_THREADS);
        cpds.setMaxIdleTime(10);
        cpds.setMaxStatementsPerConnection(4);
        cpds.setDriverClass(p.getProperty("db.driver")); //loads the jdbc driver
        cpds.setJdbcUrl(p.getProperty("db.url"));
        cpds.setUser(p.getProperty("db.user"));
        cpds.setPassword(p.getProperty("db.password"));
        Connection db = cpds.getConnection();
        Statement stmt = db.createStatement();
        stmt.execute("ALTER TABLE `reference` DISABLE KEYS");
        stmt.execute("ALTER TABLE `citation` DISABLE KEYS");
        stmt.close();
        db.close();
         
        File file = new File(args[0]);
        if (file.exists() && file.isDirectory()) {
            TrecAuditor.setStaticWriterOutput(new File(file, "STATIC_AUDIT.audit"));
            processDirectory(file);
        } else {
            throw new IllegalArgumentException("Arg must be directory");
        }
        
        threadPool.shutdown();
        while(threadPool.isTerminating()){}
        TrecAuditor.closeStaticWriter();
        
        db = cpds.getConnection();
        stmt = db.createStatement();
        stmt.execute("ALTER TABLE `reference` ENABLE KEYS");
        stmt.execute("ALTER TABLE `citation` ENABLE KEYS");
        stmt.close();
        db.close();
        DataSources.destroy(cpds);
    }

    private static void processDirectory(File dir) throws Exception {
        File[] files = dir.listFiles();
        for (File f : files) {
            String fileName = f.getName();
            if (f.isDirectory()) {
                processDirectory(f);
            } else {
                if (!fileName.endsWith(".html")
                        || !fileName.substring(0, fileName.length() - 5).matches("\\d+")) {
                    continue;
                } else {
                    //System.out.println("" + threadPool.getCompletedTaskCount());
                    TrecDocument trecDocument = new TrecDocument(f, cpds);
                    threadPool.execute(trecDocument);
                }
            }
        }
    }
}
