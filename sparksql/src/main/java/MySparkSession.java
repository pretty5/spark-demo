/*
*@ClassName:MySparkSession
 @Description:TODO
 @Author:
 @Date:2018/11/17 10:06 
 @Version:v1.0
*/
public class MySparkSession {
    public static void main(String[] args) {
        MySparkSession mySparkSession = MySparkSession
                .builder()
                .setParam1("a")
                .setParam3("b")
                .setParam2("c")
                .getOrCreate();

        MySparkSession mySparkSession1 = new MySparkSession();
        mySparkSession1.setParam1("");
        mySparkSession1.setParam2("");
        mySparkSession1.setParam3("");
        mySparkSession1.setParam4("");
    }
    private String param1;
    private String param2;
    private String param3;
    private String param4;
    private String param5;


    public static MySparkSessionBuilder builder(){
        return new MySparkSessionBuilder();
    }





    public static class MySparkSessionBuilder{
        private MySparkSession mySparkSession=new MySparkSession();

        public MySparkSessionBuilder setParam1(String param1){
                mySparkSession.setParam1(param1);
                return this;
        }
        public MySparkSessionBuilder setParam2(String param2){
            mySparkSession.setParam1(param2);
            return this;
        }
        public MySparkSessionBuilder setParam3(String param3){
            mySparkSession.setParam1(param3);
            return this;
        }
        public MySparkSessionBuilder setParam4(String param4){
            mySparkSession.setParam1(param4);
            return this;
        }
        public MySparkSessionBuilder setParam5(String param5){
            mySparkSession.setParam1(param5);
            return this;
        }
        public MySparkSession getOrCreate(){
            return mySparkSession;
        }

    }

    public String getParam1() {
        return param1;
    }

    public void setParam1(String param1) {
        this.param1 = param1;
    }

    public String getParam2() {
        return param2;
    }

    public void setParam2(String param2) {
        this.param2 = param2;
    }

    public String getParam3() {
        return param3;
    }

    public void setParam3(String param3) {
        this.param3 = param3;
    }

    public String getParam4() {
        return param4;
    }

    public void setParam4(String param4) {
        this.param4 = param4;
    }

    public String getParam5() {
        return param5;
    }

    public void setParam5(String param5) {
        this.param5 = param5;
    }



}
