import java.util.ArrayList;
import java.util.List;

/**
 * Minimal RFC-4180-style CSV line parser.
 *
 * Handles:
 *   - fields wrapped in double quotes
 *   - commas embedded inside quoted fields (e.g. book titles like
 *     "Crowner Royal (Crowner John Mystery, #13)")
 *   - escaped double quotes inside quoted fields ("" -> ")
 *
 * Assumes exactly one CSV record per input line, which is true for both
 * books.csv and reviews_100k.csv (verified: no embedded newlines in any
 * field, so Hadoop's default line-based TextInputFormat is safe to use).
 *
 * Compile this together with the job classes, e.g.:
 *   javac -classpath $(hadoop classpath) -d classes CsvUtils.java RatingHistogram.java
 */
public final class CsvUtils {

    private CsvUtils() {
    }

    public static String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++; // skip the escaped quote
                    } else {
                        inQuotes = false;
                    }
                } else {
                    current.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    fields.add(current.toString());
                    current.setLength(0);
                } else {
                    current.append(c);
                }
            }
        }
        fields.add(current.toString());
        return fields.toArray(new String[0]);
    }
}
