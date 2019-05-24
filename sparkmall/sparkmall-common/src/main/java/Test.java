import java.util.ResourceBundle;


//三个售票员卖30张票
public class Test {
    public static void main(String[] args) {

        sala01 sala01 = new sala01();
        sala01 sala02 = new sala01();
        sala01 sala03 = new sala01();

        new Thread(sala01).start();
        new Thread(sala02).start();
        new Thread(sala03).start();
    }
}

class sala01 implements Runnable{
    private static int take = 30;


    @Override
    public void run() {
        while (take>0){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            synchronized ("ss") {
                if (take > 0) {
                    take--;
                    System.out.println(Thread.currentThread().getName() + "卖了一张票，还剩" + take + "张");
                } else {
                    System.out.println("票已经卖完");
                }
            }
        }
    }
}
