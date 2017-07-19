import java.io.BufferedReader;
import java.io.InputStreamReader;

class Main
{
  public static void main(String[] args)
  {
    try
    {
      System.out.println("start");
      Process pr = Runtime.getRuntime().exec("python D:/workspace/Credan/model/test.py 000029925d904d74b6337130b1abae2b");
	  BufferedReader in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
	  String line;
	  while ((line = in.readLine()) != null)
	  {
	    System.out.println(line);
	  }
	  in.close();
      pr.waitFor();
      System.out.println("end");
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
  }
}