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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author Matt Walters <a href="mailto:team@sciplore.org">team@sciplore.org</a>
 */
public class TrecReference {

    private TrecParser parser;
    protected List<TrecCitation> citations = new ArrayList<>();
    @SuppressWarnings("unused")
	private List<Map<String, String>> highWireMapList;

    private File file;
    public TrecReference(String key, File toParse, String[] values) throws Exception {
        parser = new TrecParser();
        file = toParse;
        rawRef = values[0];
        this.key = key;
        populateReference(values[2]);
        findCitations(key, toParse);
        
        
        //o("values[0]: " + values[0]);
       // o("values[1]: " + values[1]);
        //o("values[2]: " + values[2]);
        
        
        highWireMapList = processHighWireURL(values[0]);
    }
    // reference data members
    public String rawRef;
    public String key;
    public ArrayList<String> authors = new ArrayList<>();
    public String title;
    public String date;
    public String bookTitle;
    public String pages;
    public String volume;
    public String journal;

    void addAuthor(String text) {
        authors.add(text);
    }

    void setTitle(String text) {
        title = text;
    }

    void setDate(String text) {
        date = text;
    }

    void setBookTitle(String text) {
        bookTitle = text;
    }

    void setPages(String text) {
        pages = text;
    }

    void setVolume(String text) {
        volume = text;
    }

    void setJournal(String text) {
        journal = text;
    }

    private boolean populateReference(String parsedXML) throws IOException {
        Document doc = null;
        try {
            doc = Jsoup.parse(parsedXML);
        } catch (IllegalArgumentException e) {
            o(TrecAuditor.write("Reference: attempted pass null xml to Jsoup "
                    + "aborting populateReference()" + " parsedXML: " + parsedXML, file));

            return false;
        }
        Elements authorElems = doc.select("author");
        for (Element e : authorElems) {
            addAuthor(e.text());
        }

        // get the bookTitle
        Element bt = doc.select("booktitle").first();
        if (bt != null) {
            setBookTitle(bt.text());
        }


        // get the title
        Element t = doc.select("title").first();
        if (t != null) {
            setTitle(t.text());
        }


        Element j = doc.select("journal").first();
        if (j != null) {
            setJournal(j.text());
        }


        // get the date
        Element d = doc.select("date").first();
        if (d != null) {
            setDate(d.text());
        }


        Element v = doc.select("volume").first();
        if (v != null) {
            setVolume(v.text());
        }

        // get pages
        Element p = doc.select("pages").first();
        if (p != null) {
            setPages(p.text());
        }
        return true;
    }

    private void findCitations(String key, File toParse) throws Exception {
        Matcher m = parser.getCitationMatcher(key, parser.getStringFromFile(toParse));
        while (m.find()) {
            m.group();
            TrecCitation c = new TrecCitation();
            c.setLocation(m.start());
            citations.add(c);
        }
    }

    private void o(Object obj) {
        System.out.println(obj);
    }

    private List<Map<String, String>> processHighWireURL(String referenceWithMarkup) throws MalformedURLException{
        List<Map<String, String>> toReturn = new ArrayList<>();
        Pattern pattern = Pattern.compile("<(\\s?)a href=([^>]+)>");
        Matcher matcher = pattern.matcher(referenceWithMarkup);
        while(matcher.find()){
            String result = matcher.group();
            o("regex returning: " + result);
            result = result.replaceAll("<\\s?a href=\"", "");
            result = result.replaceAll("\"\\s?>", "");
            result = "file:/" + result;
            o("determined URL " + result);
            URL url = new URL(result);
            String query = url.getQuery();
            String[] params = query.split("&");
            for(String param : params){
                Map<String, String> map = new HashMap<>();
                map.put("URL", result);
                String[] keyValuePair = param.split("=");
                o("mapping key: \"" + keyValuePair[0] + "\" to value: \"" + keyValuePair[1] + "\"");
                map.put(keyValuePair[0], keyValuePair[1]);
                toReturn.add(map);
            }
        }
        return toReturn;
    }
    
    
}
