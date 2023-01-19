import java.util.StringTokenizer;

class temp{

    public static void main(String args[]){
    
    String s = "53877 20011230  1.006  -82.61   35.49     2.5    -7.5    -2.5    -3.2 -9999.0     9.49 R -9999.0 -9999.0    -1.7 -9999.0 -9999.0 -9999.0 -99.000 -99.000 -99.000 -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0";
    
    String sub = s.substring(38,46);
    System.out.println(sub.trim());
    
    
    String result[] = s.split("\\s");
    
    System.out.println(result[6]);
    }

}
