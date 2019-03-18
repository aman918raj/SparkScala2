package HomeBudget;

import java.awt.*;
import java.io.*;
import java.util.ArrayList;

public class JavaHtmlWriter {

    public void createUI(ArrayList list) throws IOException {

        File fileRead = new File("/Users/amaraj0/Documents/MyData/budget/html_test.html");
        FileReader fr = new FileReader(fileRead);
        BufferedReader br = new BufferedReader(fr);
        File fileWrite = new File("/Users/amaraj0/Documents/MyData/budget/new_test.html");
        FileWriter fw = new FileWriter(fileWrite);
        BufferedWriter bw = new BufferedWriter(fw);

        String line = br.readLine();
        while (line != null){

            if (line.contains("replace this line")){
                bw.write("<td>"+list.get(0)+"</td>\n" +
                        "    <td>"+list.get(1)+"</td>\n" +
                        "    <td>"+list.get(2)+"</td>\n" +
                        "    <td>"+list.get(3)+"</td>\n" +
                        "    <td>"+list.get(4)+"</td>\n" +
                        "    <td>"+list.get(5)+"</td>\n" +
                        "    <td>"+list.get(6)+"</td>\n" +
                        "    <td>"+list.get(7)+"</td>\n" +
                        "    <td>"+list.get(8)+"</td>");
            }
            else{
                bw.write(line);
            }
            line = br.readLine();
        }
        bw.close();
        br.close();

        Runtime.getRuntime().exec("/Users/amaraj0/Documents/MyData/budget/run_html.sh");
    }
}
