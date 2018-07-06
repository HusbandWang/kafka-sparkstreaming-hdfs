import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class MyProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("metadata.broker.list","10.10.171.63:9092,10.10.142.7:9092,10.10.11.79:9092");
        props.setProperty("serializer.class","kafka.serializer.StringEncoder");
        props.put("request.required.acks","1");
        ProducerConfig config = new ProducerConfig(props);
        //创建生产这对象
        Producer<String, String> producer = new Producer<String, String>(config);
        //生成消息
        try {
            int i =1;
            while(i < 100){
                //发送消息
               // KeyedMessage<String, String> data = new KeyedMessage<String, String>("zhuishushenqi","{\"name\":["+i+"\"SpringMVC\",\"Mybatis \",\"Freemarker\",\"Shiro\"],\"age\": [\"Redis\",\"RDS\",\"七牛云存储\"]}\n");
                KeyedMessage<String, String> data = new KeyedMessage<String, String>("zhuishushenqi","{\"name\":["+i+"\"SpringMVC\",\"Mybatis \",\"Freemarker\",\"Shiro\"],\"age\": [\"Redis\",\"RDS\",\"七牛云存储\"]}\n");

                producer.send(data);
                i ++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
}