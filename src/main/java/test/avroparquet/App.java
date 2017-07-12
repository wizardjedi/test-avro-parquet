/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test.avroparquet;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ConfigurationUtil;

/**
 *
 * @author wiz
 */
public class App {

    public static final Random random = new Random(System.currentTimeMillis());

    public static String[] msgWords = new String[] {
        "23",
"24",
"28",
"30",
"(NOTAM)",
"P-8A",
"Poseidon",
"RC-135V",
"а",
"авиабаз",
"авиации.",
"авиационного",
"«Адмирал",
"акваторией",
"акватории",
"боевиков",
"боеприпасов",
"бомбардировщиков",
"были",
"в",
"В",
"вблизи",
"ВМФ",
"военной",
"Военные",
"Воздушно-космических",
"воздушными",
"вооружения",
"восточной",
"вылетевшие",
"выпустила",
"где",
"государство»",
"Григорович»",
"группировки",
"данные",
"для",
"закрыт",
"(запрещенной",
"Затем",
"и",
"ИГ",
"из",
"извещениям",
"«Интерфакс»",
"«Исламское",
"их",
"июня.",
"июня",
"«Калибр»",
"кораблей",
"корабли",
"который",
"«Краснодар»",
"Крите",
"крупные",
"крылатые",
"крылатых",
"лодка",
"мореплавателей",
"моря.",
"моря",
"на",
"навигационного",
"над",
"находятся",
"Об",
"объектам",
"объектов",
"остатки",
"осуществили",
"осуществлены",
"осуществляли",
"отслеживающих",
"очередные",
"патрульный",
"передвижение",
"персонала",
"по",
"По",
"побережья",
"подводная",
"подводного",
"Подлодка",
"полеты",
"положения.",
"предупреждения",
"проведением",
"провели",
"провинции",
"противолодочный",
"пункты",
"пуски",
"Пуски",
"пусков",
"разведчик",
"разведывательные",
"районом",
"ракет",
"ракетных",
"ракеты",
"результате",
"России.",
"России",
"России)",
"с",
"сайтов",
"самолет",
"самолеты",
"сведениям",
"связи",
"сил.",
"Сирии.",
"Сирии",
"Сицилии",
"склады",
"со",
"согласно",
"сообщает",
"соответственно",
"Средиземного",
"ссылкой",
"стратегический",
"субботу",
"США",
"также",
"террористов",
"удара",
"ударами",
"уничтожены",
"управления",
"Хама.",
"части",
"шести",
"Эссен»",
"этом"
    };

    public static void main(String[] args) throws IOException {
        Distribution distribution = genDistribution(0);

        DatumWriter<Distribution> userDatumWriter = new SpecificDatumWriter<Distribution>(Distribution.class);
        DataFileWriter<Distribution> dataFileWriter = new DataFileWriter<Distribution>(userDatumWriter);
        dataFileWriter.create(distribution.getSchema(), new File("distribution.avro"));

        FileWriter fw = new FileWriter(new File("distribution.csv"));

        // choose compression scheme
        CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;

        // set Parquet file block size and page size values
        int blockSize = 256 * 1024 * 1024;
        int pageSize = 64 * 1024;

        Path outputPath = new Path("file:///tmp/parquet/");

        Schema avroSchema = distribution.getSchema();

        ParquetWriter<Distribution> parquetWriter;
        parquetWriter =
                new AvroParquetWriter<Distribution>(
                        outputPath, avroSchema, compressionCodecName, blockSize, pageSize
                );



        for (int i = 1; i < 10 * 1000 * 1000 ; i++) {

            Distribution d = genDistribution(i);

            dataFileWriter.append(d);

            fw.append(distribution2csv(d)).append('\n');

            parquetWriter.write(d);

            if (i % 1000 == 1) {
                System.out.println("processed=" + i);
            }
        }

        parquetWriter.close();
        dataFileWriter.close();
        fw.close();

    }

    public static Distribution genDistribution(int i) {
        float deliveryRate = random.nextFloat();

        int partsCount = random.nextInt(5) + 1;

        int words = 1 + partsCount * 70 / 10;

        String[] msg = new String[words];

        for (int j=0;j<words;j++) {
            msg[j] = msgWords[random.nextInt(msgWords.length)];
        }

        int acceptedCount;
        int pendingCount;
        int deliveredCount;
        int rejectedCount;

        Long sendDate = System.currentTimeMillis();

        Long deliveryDate = System.currentTimeMillis();

        long addDate = System.currentTimeMillis();

        if (deliveryRate <= 0.9) {
            // delivered

            deliveredCount = random.nextInt(partsCount);
            rejectedCount = random.nextInt(partsCount - deliveredCount);
            acceptedCount = random.nextInt(partsCount - deliveredCount - rejectedCount);
            pendingCount = partsCount - deliveredCount - rejectedCount - acceptedCount;

            sendDate += random.nextInt(365*24*3600*1000);
            deliveryDate += random.nextInt(365*24*3600*1000);
        } else if (deliveryRate <= 0.95) {
            acceptedCount = random.nextInt(partsCount);
            pendingCount = partsCount - acceptedCount;
            deliveredCount = 0;
            rejectedCount = 0;

            sendDate += random.nextInt(365*24*3600*1000);
            deliveryDate = null;
        } else {
            acceptedCount = 0;
            pendingCount = partsCount;
            deliveredCount = 0;
            rejectedCount = 0;

            sendDate = null;
            deliveryDate = null;
        }

        Distribution d
                = Distribution
                .newBuilder()
                .setId(i)
                .setTransactionId(i * 10000)
                .setAbonent(String.valueOf(79000000000l+random.nextInt(9999999)))
                .setSender("default")
                .setSmsText(String.join(" ", msg))
                .setAddDate(addDate)
                .setSendDate(sendDate)
                .setDeliveryDate(deliveryDate)
                .setPendingCount(pendingCount)
                .setAcceptedCount(acceptedCount)
                .setDeliveredCount(deliveredCount)
                .setRejectedCount(rejectedCount)
                .setError(random.nextBoolean() ? null : "13")
                .build();

        return d;
    }

    public static String distribution2csv(Distribution d) {
        String[] parts = new String[]{
            String.valueOf(d.getId()),
            String.valueOf(d.getTransactionId()),
            String.valueOf(d.getAbonent()),
            String.valueOf(d.getSender()),
            String.valueOf(d.getSmsText()),
            String.valueOf(d.getAddDate()),
            String.valueOf(d.getSendDate()),
            String.valueOf(d.getDeliveryDate()),
            String.valueOf(d.getPendingCount()),
            String.valueOf(d.getAcceptedCount()),
            String.valueOf(d.getDeliveredCount()),
            String.valueOf(d.getRejectedCount()),
            String.valueOf(d.getError())
        };

        return String.join(";", parts);
    }
}
