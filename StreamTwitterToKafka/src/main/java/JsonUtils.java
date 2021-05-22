import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;

public class JsonUtils {
    
    static final JSONObject getJsonFromFile(String filePath) {
        JSONParser parser = new JSONParser();

        JSONObject jsonObject = null;
        try {
            Object obj = parser.parse(new FileReader(filePath));
            jsonObject = (JSONObject) obj;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return jsonObject;
    }

}
