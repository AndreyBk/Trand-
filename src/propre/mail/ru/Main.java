package propre.mail.ru;
//branche master
// TODO: 16.11.2018 убить таймер. Посмотреть жизненный цикл активности

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sun.javafx.logging.Logger;
import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.text.Text;
import javafx.stage.Stage;

import java.lang.reflect.Array;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.LogManager;
//import java.util.logging.Logger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import sun.rmi.runtime.Log;

//import org.slf4j.Logger;
//import org.apache.log4j.Logger;

public class Main extends Application {
    private static Button _btn;
    private static Text _actiontarget;
    static LogManager _lm;
    //    static Logger _log;
//    private static Logger LOGGER;
    static TextField _tf_sin1, _tf_sin2, _tf_line, _tf_meandr;
    static String _s_sin1 = "", _s_sin2 = "", _s_line = "",_s_meandr="";
    static String _s_value="";
    static JsonObject _j_result, _j_one_tag;
    static JsonArray _j_array;
    static TemporalAccessor _ta;
    static long _time;
    static Timer _timer;
    static TimerTask_1_second _timerTask_1_second;
    static Double  _d_line=0.0, _d_sin1=0.0,_d_sin2=0.0, _d_meandr=0.0;
    static int _degrees_sin1=0, _degrees_sin2=0, _degrees_line=0, _degrees_meandr=0;
    static TestCallback callback;
    static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
//        _btn = new Button();
//        _btn.setText("Send");

        _actiontarget = new Text();
//        _ip=new TextField();
        _tf_sin1 = new TextField();
        _tf_sin2=new TextField();
        _tf_line = new TextField();
        _tf_meandr = new TextField();

        _j_result=new JsonObject();
        _j_one_tag=new JsonObject();
        _j_array=new JsonArray();

        _ta = Clock.systemUTC().instant();
//        _time = ((Instant) _ta).getEpochSecond();

        _timer = new Timer();//подключение таймера
        _timerTask_1_second = new TimerTask_1_second();
        _timer.schedule(_timerTask_1_second, 1000, 1000);//

        System.out.println("*********************");
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) throws Exception {

        Parent root = FXMLLoader.load(getClass().getResource("activity.fxml"));
        Scene scene = new Scene(root, 600, 275);
        primaryStage.setTitle("Generator to Kafka");
        primaryStage.setScene(scene);
        primaryStage.show();

//        _actiontarget = (Text) scene.lookup("#actiontarget");
        _tf_sin1 = (TextField) scene.lookup("#sin1");
        _tf_sin1.setText("DOPSIGNALS.test1.value1.Value");
        _tf_sin2 = (TextField) scene.lookup("#sin2");
        _tf_sin2.setText("DOPSIGNALS.test1.value2.Value");
        _tf_line = (TextField) scene.lookup("#line");
        _tf_line.setText("DOPSIGNALS.test1.value3.Value");
        _tf_meandr= (TextField) scene.lookup("#meandr");
        _tf_meandr.setText("DOPSIGNALS.test1.value4.Value");


//        _btn.setOnAction(new EventHandler<ActionEvent>() {
//            @Override
//            public void handle(ActionEvent e) {
//                _actiontarget.setText("Topic: "+_s_topic + " Value: " + _s_value);
//            }
//        });

//        primaryStage.setTitle("JavaFX Welcome");
//        GridPane grid = new GridPane();
//        grid.setAlignment(Pos.CENTER);
//        grid.setHgap(10);
//        grid.setVgap(10);
//        grid.setPadding(new Insets(25, 25, 25, 25));
//
//        Text scenetitle = new Text("Welcome");
//        scenetitle.setId("welcome-text");
//        grid.add(scenetitle, 0, 0, 2, 1);
//
//        Label userName = new Label("User Name:");
//        grid.add(userName, 0, 1);
//
//        TextField userTextField = new TextField();
//        grid.add(userTextField, 1, 1);
//
//        Label pw = new Label("Password:");
//        grid.add(pw, 0, 2);
//
//        PasswordField pwBox = new PasswordField();
//        grid.add(pwBox, 1, 2);
//
//        Button btn = new Button("Sign in");
//        HBox hbBtn = new HBox(10);
//        hbBtn.setAlignment(Pos.BOTTOM_RIGHT);
//        hbBtn.getChildren().add(btn);
//        grid.add(hbBtn, 1, 4);
//
//        final Text actiontarget = new Text();
//        grid.add(actiontarget, 0, 6);
//        grid.setColumnSpan(actiontarget, 2);
//        grid.setHalignment(actiontarget, HPos.RIGHT );
//        actiontarget.setId("actiontarget");
//
//        btn.setOnAction(new EventHandler<ActionEvent>() {
//
//            @Override
//            public void handle(ActionEvent e) {
//                actiontarget.setText("Sign in button pressed");
//            }
//        });


//        Scene scene = new Scene(grid, 300, 275);
//        primaryStage.setScene(scene);
////        scene.getStylesheets().add(Login.class.getResource("Login.css").toExternalForm());
//        primaryStage.show();
    }

    static void send_kafka(String _value) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "95.163.213.180:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        TestCallback callback = new TestCallback();
        ProducerRecord<String, String> data = new ProducerRecord<String, String>("nifi-test", null, _value);
        producer.send(data, callback);

        producer.flush();
        producer.close();

    }

    private static class TestCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                String _message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                _actiontarget.setText(_message);
                //     _actiontarget.setText(_s_topic+" "+_s_value);

                System.out.println(_message);
            }
        }
    }





    static class TimerTask_1_second extends TimerTask {
        TimerTask_1_second() {//конструктор
        }

        @Override
        public void run() {
            if (_degrees_sin1>360) _degrees_sin1=0;
            else _degrees_sin1++;
            _d_sin1=Math.sin(Math.toRadians(_degrees_sin1))*100;
            long _s=Math.round(_d_sin1*100);
            _d_sin1= Double.valueOf(_s/100);

            if (_degrees_sin2>360) _degrees_sin2=0;
            else _degrees_sin2=_degrees_sin2+2;
            _d_sin2=Math.sin(Math.toRadians(_degrees_sin2))*100;
            _s=Math.round(_d_sin2*100);
            _d_sin2= Double.valueOf(_s/100)+15;

            if (_degrees_line>360) _degrees_line=0;
            else _degrees_line=_degrees_line+3;
            if (_degrees_line<180)  _d_line=_d_line+0.5;
            else _d_line=_d_line-0.5;
            _s=Math.round(_d_line*100);
            Double _d_l= Double.valueOf(_s/100);

            if (_degrees_meandr>360) _degrees_meandr=0;
            else _degrees_meandr=_degrees_meandr+4;
            if (_degrees_meandr<180)  _d_meandr=75.0;
            else _d_meandr=12.0;

            _ta = Clock.systemUTC().instant();
            _time = ((Instant) _ta).getEpochSecond();
            _j_array=new JsonArray();

            _s_sin1 = _tf_sin1.getText();
            _s_sin2 = _tf_sin2.getText();
            _s_line = _tf_line.getText();
            _s_meandr = _tf_meandr.getText();

            _j_one_tag=new JsonObject();
            _j_one_tag.addProperty("id",_s_sin1);
            _j_one_tag.addProperty("vd",Double.parseDouble(String.valueOf(_d_sin1)));
            _j_one_tag.addProperty("q",0);
            _j_one_tag.addProperty("ts",_time);
            _j_array.add(_j_one_tag);

            _j_one_tag=new JsonObject();
            _j_one_tag.addProperty("id",_s_sin2);
            _j_one_tag.addProperty("vd",Double.parseDouble(String.valueOf(_d_sin2)));
            _j_one_tag.addProperty("q",0);
            _j_one_tag.addProperty("ts",_time);
            _j_array.add(_j_one_tag);

            _j_one_tag=new JsonObject();
            _j_one_tag.addProperty("id",_s_line);
            _j_one_tag.addProperty("vd",Double.parseDouble(String.valueOf(_d_l)));
            _j_one_tag.addProperty("q",0);
            _j_one_tag.addProperty("ts",_time);
            _j_array.add(_j_one_tag);

            _j_one_tag=new JsonObject();
            _j_one_tag.addProperty("id",_s_meandr);
            _j_one_tag.addProperty("vd",Double.parseDouble(String.valueOf(_d_meandr)));
            _j_one_tag.addProperty("q",0);
            _j_one_tag.addProperty("ts",_time);
            _j_array.add(_j_one_tag);

            _j_result.add("values", _j_array);
            _s_value=_j_result.toString();

            System.out.println(_j_result);

            send_kafka(_s_value);
        }
    }
}
