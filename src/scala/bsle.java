import java.io.Serializable;

public class bsle implements java.io.Serializable {
    private String name;
    private String age;



    public void setName(String name){
        this.name = name;
    }
    public void setAge(String age){
        this.age = age;
    }
    public String getName(){
        return this.name;
    }
    public String getAge(){
        return this.age;
    }



}
