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

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.sciplore.citrec.Helper;

/**
 *
 * @author Matt Walters <a href="mailto:team@sciplore.org">team@sciplore.org</a>
 */
public class TrecParser {

    private File file;
    private String html;
    private File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    private static File parsCitRoot;
    private String anchorForm;
    private String referenceCache;
    private Matcher referenceMatcher;
    private String parsCitXML;

    public TrecParser() throws IOException {
        parsCitRoot = new File(Helper.getProperties().getProperty("parsCitRoot"));
        
    }

    public Map<String, String[]> getKeyRefMap(File toParse) throws Exception {
        file = toParse;
        html = getStringFromFile(file);
        anchorForm = determineAnchorForm(html);
        referenceCache = "";

        // the key to this map is the anchor key.

        // the value is a String[] where whose elements are:
        // 0 -> reference with markup
        // 1 -> reference without markup
        // 2 -> parsCit output
        Map<String, String[]> keyRefMap = new HashMap<>();

        referenceMatcher = getReferenceMatcher(anchorForm, html);

        while (referenceMatcher.find()) {
            String refernceWithMarkup = referenceMatcher.group();
            refernceWithMarkup = clipHIGHWIRETail(refernceWithMarkup);
            String referernce = Jsoup.parse(refernceWithMarkup).text().replaceAll("\\[.+\\]", "");
            String[] strings = {refernceWithMarkup, referernce, null};
            String key = getKey(anchorForm, refernceWithMarkup);
            keyRefMap.put(key, strings);
            referenceCache += (referernce.replace("\n", " "));
            if (!referenceMatcher.hitEnd()) {
                referenceCache += "\n";
            }
        }

        parsCitXML = parsCit(referenceCache);

        injectCitationXML(keyRefMap, parsCitXML);

        return keyRefMap;

    }

    public void injectCitationXML(Map<String, String[]> keyRefMap, String parsCitXML) throws IOException {
        Elements elems = null;
        try {
            elems = Jsoup.parse(parsCitXML).select("citation");
        } catch (IllegalArgumentException e) {
            TrecAuditor.write("failed to inject html", file);
            return;
        }
        for (Element e : elems) {

            double highestScore = -1.0;

            String currentKey = null;
            String currentElem = null;
            for (String key : keyRefMap.keySet()) {
                String reference = keyRefMap.get(key)[1];
                double score = getScore(reference, e.text());
                if (score > highestScore) {
                    highestScore = score;
                    currentKey = key;
                    currentElem = e.toString();
                }

            }


            o(TrecAuditor.write("determined match with scrore " + highestScore, file));
            try {

                if (highestScore < .75) {
                    String s = "possible mismatch! score: " + highestScore + "\txml: " 
                            + currentElem.replace("\n", "\n\t") + "\treference: " 
                            + keyRefMap.get(currentKey)[0];
                    o(TrecAuditor.write(s, file));
                    continue;
                }
                String[] s = keyRefMap.get(currentKey);
                s[2] = currentElem;
                keyRefMap.remove(currentKey);
                keyRefMap.put(currentKey, s);
            } catch (Exception ex) {
                TrecAuditor.write("count not get reference from keyRefMap: " + keyRefMap.toString(), file);
                
            }




        }




    }

    public String getStringFromFile(File f) throws Exception {

        int bufferSize = 2048;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buffer = new byte[bufferSize];
        int len = 0;
        FileInputStream fis = new FileInputStream(f);
        while ((len = fis.read(buffer)) != -1) {
            bos.write(buffer, 0, len);
        }
        fis.close();

        return new String(bos.toByteArray());

    }

    public String parsCit(String input) throws IOException, InterruptedException, Exception {
        File tmpInputFile = createUniqueFile(input + "\n");
        ProcessBuilder pb = new ProcessBuilder("perl", "-CSD",
                parsCitRoot.getAbsolutePath() + File.separator + "parseRefStrings.pl",
                tmpInputFile.getName());
        pb.directory(tmpDir);
        Process p = pb.start();
        String toReturn = "";
        try {
            toReturn = new Scanner(p.getInputStream()).useDelimiter("\\A").next();
        } catch (Exception e) {
            TrecAuditor.write("could not read from process: " + toReturn, file);
        }
        tmpInputFile.delete();
        return toReturn;
    }

    public File createUniqueFile(String s) throws IOException {
        File f = File.createTempFile("parsCit", ".temp", tmpDir);
        BufferedWriter writer = new BufferedWriter(new FileWriter(f));
        writer.write(s);
        writer.close();
        assert f.exists();
        return f;
    }

    public String determineAnchorForm(String html) throws IOException {
        String[] formats = {
            "BB\\d+",
            "(?<!BI)B\\d+",
            "REF\\d+",
            "R\\d+",
            "[A-Z_\\-]+\\d{4}",
            "BIB\\d+",
            "\\w{3}\\d+C\\d+",
            "\\w+\\-\\d{2}\\-\\d{2}\\-\\d{2}\\-\\w+\\d?"
        };

        // create occurrences map and populate with initial values of 0;
        Map<String, Double> occurrences = new HashMap<>();
        for (String s : formats) {
            occurrences.put(s, 0.0);
        }

        // create integer to hold total matches for all formats
        double totalOccurrences = 0;

        // go pull out occurecnes for each format
        for (String s : formats) {
            Matcher refMatcher = getReferenceMatcher(s, html);

            while (refMatcher.find()) {
                occurrences.put(s, occurrences.get(s) + 1);
                totalOccurrences++;
            }
        }

        // find the format with the highest occurences
        double largestToDate = 0.0;
        String mostFrequentFormat = null;
        for (String s : formats) {

            if (occurrences.get(s) > largestToDate) {
                largestToDate = occurrences.get(s);
                mostFrequentFormat = s;
            }
        }

        // make sure totalOccurences isn't zero before we start dividing by it.
        // wouldn't want the universe to implode or something now would we...
        // .... while were at it, lets make sure there was enough data to be confident
        // ... magic number is.... 2!
        int totalOccurencesLowerBound = 2;
        if (totalOccurrences < totalOccurencesLowerBound) {
            return null;
        }

        double freqencyOfMostFrequent = largestToDate / totalOccurrences;

        // if this does not indicate a majority ( > .50) return null   /// this could probably be a tighter figure.
        // since we changed this match the refernces rather than the anchors, most htmls docs match with 1.0
        if (freqencyOfMostFrequent < 0.5) {

            return null;
        }

        // at this point we should be pretty confident that "mostFrequentFormat" is out winner
        return mostFrequentFormat;
    }

    public Matcher getReferenceMatcher(String anchorPattern, String html) throws IOException {

        String s = "<a name\\s?=\\s?\"?\\s?" + anchorPattern + "\\s?\"?(?:(?!<a name\\s?=\\s?\"?" + anchorPattern + ").)*";
        Pattern p = Pattern.compile(s, Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
        return p.matcher(html);
    }

    public Matcher getCitationMatcher(String key, String html) {
        // this needs major extension.
        String s = "href\\s?=\\s?\"?#" + key;
        Pattern p = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
        return p.matcher(html);
    }

    public String getKey(String anchorPattern, String reference) {
        Pattern p = Pattern.compile(anchorPattern, Pattern.CASE_INSENSITIVE);
        Matcher matcher = p.matcher(reference);
        if (matcher.find()) {
            return matcher.group();
        } else {
            return null;
        }
    }

    public double getScore(String reference, String parsedXML) {

        List<String> refWords = getWords(reference);
        List<String> xmlWords = getWords(parsedXML);

        double totalRefWords = refWords.size();
        double totalRefWordMatches = 0.0;
        for (String refWord : refWords) {
            if (parsedXML.contains(refWord)) {
                totalRefWordMatches++;
            }
        }

        double totalXMLWords = xmlWords.size();
        double totalXMLWordMatches = 0.0;
        for (String xmlWord : xmlWords) {
            if (reference.contains(xmlWord)) {
                totalXMLWordMatches++;
            }
        }
        return ((totalRefWordMatches / totalRefWords) + (totalXMLWordMatches / totalXMLWords))
                / 2.0;

    }

    public List<String> getWords(String ref) {
        List<String> l = new ArrayList<>();
        Matcher m = Pattern.compile("[a-zA-Z]+\\b", Pattern.CASE_INSENSITIVE).matcher(ref);
        while (m.find()) {
            String word = m.group();
            l.add(word);
        }
        return l;
    }

    public void o(Object text) {
        //System.out.println(text);
    }

    private String clipHIGHWIRETail(String refernceWithMarkup) {
        String s = "/HIGHWIRE";
        int index = refernceWithMarkup.indexOf("/HIGHWIRE");
        if (index == -1) {
            return refernceWithMarkup;
        } else {
            return refernceWithMarkup.substring(0, index + s.length());
        }
    }
}
