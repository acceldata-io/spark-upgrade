package com.acceldata.openrewrite.spark;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What Spark 3.5.5 does differently from 2.4 with a datetime pattern (2.4:
 * SimpleDateFormat; 3.x: DateTimeFormatter). A port of pyspark-rules'
 * {@code datetime_rules.check_pattern} and the Scalafix rules'
 * {@code fix.support.DateTimePatterns}, whose claims were run on 3.5.5, so
 * the three paths judge a pattern
 * alike.
 */
final class DateTimePatterns {

    private DateTimePatterns() {
    }

    private static final Map<Character, String> WEEK_BASED = new LinkedHashMap<>();
    private static final Map<Character, String> PARSING_ONLY = new LinkedHashMap<>();
    private static final Map<Character, Integer> MAX_RUN = new LinkedHashMap<>();
    private static final String[] TOO_LONG = {"GGGGG", "MMMMM", "LLLLL", "EEEEE", "yyyyyyy"};
    private static final Pattern RUN = Pattern.compile("([A-Za-z])\\1*|[^A-Za-z]");

    static {
        WEEK_BASED.put('Y', "the week-based year -- almost always meant as `y`, the calendar year");
        WEEK_BASED.put('w', "week of the week-based year -- use weekofyear() instead");
        WEEK_BASED.put('W', "week of month -- no 3.x pattern letter means this");
        WEEK_BASED.put('u', "day number of the week in 2.4 (1 = Monday) -- use dayofweek() or `E`");
        PARSING_ONLY.put('E', "the day-of-week name");
        PARSING_ONLY.put('F', "the week of month (2.4) / aligned day-of-week-in-month (3.x)");
        String letters = "ahHkKmsdDFzZ";
        int[] limits = {1, 2, 2, 2, 2, 2, 2, 2, 3, 1, 4, 5};
        for (int i = 0; i < letters.length(); i++) {
            MAX_RUN.put(letters.charAt(i), limits[i]);
        }
    }

    /** Runs of one repeated letter outside quoted text, as both formatters tokenise. */
    private static List<String> runs(String pattern) {
        List<String> out = new ArrayList<>();
        String[] parts = pattern.split("'", -1);
        for (int i = 0; i < parts.length; i += 2) {
            Matcher m = RUN.matcher(parts[i]);
            while (m.find()) {
                out.add(m.group());
            }
        }
        return out;
    }

    /** Why Spark 3.5.5 treats {@code pattern} differently from 2.4, or null.
     * {@code parsing} is true for to_date/to_timestamp/unix_timestamp. */
    static String check(String pattern, boolean parsing) {
        List<String> runs = runs(pattern);
        Set<Character> letters = new HashSet<>();
        for (String r : runs) {
            if (Character.isLetter(r.charAt(0))) {
                letters.add(r.charAt(0));
            }
        }
        for (Map.Entry<Character, String> e : WEEK_BASED.entrySet()) {
            if (letters.contains(e.getKey())) {
                return "uses '" + e.getKey() + "', " + e.getValue() + ". Spark 3.0+ rejects week-based pattern letters outright, so the call fails";
            }
        }
        if (parsing) {
            for (Map.Entry<Character, String> e : PARSING_ONLY.entrySet()) {
                if (letters.contains(e.getKey())) {
                    return "uses '" + e.getKey() + "' (" + e.getValue() + ") to parse. Spark 3.0+ can format with it but refuses to parse with it";
                }
            }
        }
        for (String style : TOO_LONG) {
            for (String r : runs) {
                if (r.contains(style)) {
                    return "uses '" + style + "'. Spark 3.0+ refuses this length (its meaning changed in DateTimeFormatter)";
                }
            }
        }
        for (String r : runs) {
            Integer limit = MAX_RUN.get(r.charAt(0));
            if (limit != null && r.length() > limit) {
                return "repeats '" + r.charAt(0) + "' " + r.length() + " times. 2.4's SimpleDateFormat padded to any width; " +
                        "Spark 3.x's DateTimeFormatter allows at most " + limit + ", so the pattern is rejected";
            }
        }
        for (String c : new String[]{"#", "{", "}"}) {
            if (runs.contains(c)) {
                return "contains '" + c + "', reserved by Spark 3.x's DateTimeFormatter, so the pattern is rejected";
            }
        }
        if (runs.contains("[") || runs.contains("]")) {
            return "contains '[' or ']', literal text in 2.4 but an optional section in Spark 3.x";
        }
        if (!parsing && letters.contains('F')) {
            return "uses 'F', which meant week-of-month in 2.4 and means aligned-day-of-week-in-month from 3.0 -- same pattern, different result";
        }
        if (parsing && letters.contains('h') && !letters.contains('a')) {
            return "parses the 12-hour clock 'h' with no 'a' (am/pm) marker. 2.4 parsed 00-23 leniently; Spark 3.x returns NULL for any hour outside 1-12";
        }
        return null;
    }
}
