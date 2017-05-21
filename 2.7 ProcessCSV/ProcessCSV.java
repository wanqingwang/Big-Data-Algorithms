package processingcsv.dp;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;



public class ProcessCSV {
      @SuppressWarnings({ "resource", "deprecation" })
	public static void main(String[] args) {
            try {
            String fName = "/home/cloudera/arbres.csv";
            String thisLine;
            //int count = 0;
            FileInputStream fis;
                fis = new FileInputStream(fName);
            DataInputStream myInput = new DataInputStream(fis);
            //int i = 0;
            File file = new File ("output.txt");
            PrintStream printStreamToFile = new PrintStream(file);
            System.setOut(printStreamToFile);
            while ((thisLine = myInput.readLine()) != null) {
                String strar[] = thisLine.split(";");
                System.out.println("Year - " + strar[5] + " , Height-" + strar[6]);
                             
                }
              }catch(IOException e){
                  e.printStackTrace();
              }
        
}
}    