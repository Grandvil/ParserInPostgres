import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class CreateTable {

    //Основной метод приложения
    public static void main(String[] args) throws IOException, SQLException {
        Date start = new Date();
        System.out.println(start);
        //String filePath = "/Users/Grandvil/Downloads/3328496339_40702810010000001390_002_0519.txt";
//        String filePath = "/Users/Grandvil/Downloads/parse-2.txt";
        String filePath = ".\\parse-2.txt";
        List<PersonalAccount> personalAccounts = parseProductCsv(filePath);

        DB db = new DB();
        Connection connection = db.connect();
        Statement statement = connection.createStatement();
        int max_id_date = 0;
        for (PersonalAccount pa : personalAccounts) {
            String selectQuery = String.format("select id from public.personal_account where id = %s", pa.id);
            ResultSet resultSet = statement.executeQuery(selectQuery);
            int id = 0;
            while (resultSet.next()) {
                id = resultSet.getInt(1);
            }
            if (id > 0) continue;
            String query = String.format("insert into public.personal_account(id, full_name, address) values " +
                    "(%s, '%s', '%s')", pa.id, pa.name, pa.address);
            statement.execute(query);



            String insertIndicationDate = String.format("INSERT INTO indications_date(date, pa_id) VALUES (%s, %s)", pa.iDate.date, pa.id);
            statement.execute(insertIndicationDate);

            if (max_id_date == 0) {
                resultSet = statement.executeQuery("SELECT max(id) FROM indications_date");

                while (resultSet.next()) {
                    max_id_date = resultSet.getInt(1);
                }
                if (max_id_date == 0) {
                    max_id_date = 1;
                }
            }

            String insertIndicators = String.format("INSERT INTO indications(indicator_id, i_value) VALUES (%s, %s);", max_id_date, pa.iDate.indi.indicatorValue);
            statement.execute(insertIndicators);
            max_id_date++;
        }

        Date end = new Date();
        System.out.println(end.getTime() - start.getTime());
    }

    //Расинг CSV файла по указанному пути и получение продуктов из него
    private static List<PersonalAccount> parseProductCsv(String filePath) throws IOException {
        //Загружаем строки из файла
        List<PersonalAccount> products = new ArrayList<PersonalAccount>();
        List<String> fileLines = null;
        try {
            fileLines = Files.readAllLines(Paths.get(filePath));
        } catch (Exception E) {
            System.out.println(E);
        }

        Random rand = new Random();

        for (String fileLine : fileLines) {
            String[] splitedText = fileLine.split(";");
            ArrayList<String> columnList = new ArrayList<String>();
            for (int i = 0; i < splitedText.length; i++) {
                //Если колонка начинается на кавычки или заканчиваеться на кавычки
                if (isColumnPart(splitedText[i])) {
                    String lastText = columnList.get(columnList.size() - 1);
                    columnList.set(columnList.size() - 1, lastText + ";" + splitedText[i]);
                } else {
                    columnList.add(splitedText[i]);
                }
            }
            try {
                PersonalAccount pa = new PersonalAccount();
                pa.id = Integer.parseInt(columnList.get(0));
                pa.name = columnList.get(1);
                pa.address = columnList.get(2);

                if (columnList.size() > 3) {
                    IndicationsDate iDate = new IndicationsDate();
                    iDate.personalAccountId = pa.id;
                    iDate.date = columnList.get(3);
                    iDate.id = rand.nextInt();
                    pa.iDate = iDate;
                    if (columnList.size() > 4) {
                        Indications indi = new Indications();
                        indi.indicationsDateId = iDate.id;
                        indi.indicatorValue = Double.parseDouble(columnList.get(4));
                        pa.iDate.indi = indi;
                    }
                }
                products.add(pa);
            } catch (NumberFormatException e) {
                System.out.println(e);
                continue;
            }
            //System.out.println(" ");
        }
        return products;
    }

    //Проверка является ли колонка частью предыдущей колонки
    private static boolean isColumnPart(String text) {
        String trimText = text.trim();
        //Если в тексте одна ковычка и текст на нее заканчиваеться значит это часть предыдущей колонки
        return trimText.indexOf("\"") == trimText.lastIndexOf("\"") && trimText.endsWith("\"");
    }
}