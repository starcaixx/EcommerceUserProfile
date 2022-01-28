package etl;

import java.time.LocalDate;
import java.time.Month;

public class MyTest {
    public static void main(String[] args) {
        LocalDate now = LocalDate.of(2020, Month.NOVEMBER, 30);

        LocalDate sevenDateAgo = now.plusDays(-7);
        System.out.println(sevenDateAgo);
    }
}
