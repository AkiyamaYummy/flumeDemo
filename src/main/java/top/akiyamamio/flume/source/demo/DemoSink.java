package top.akiyamamio.flume.source.demo;

import org.apache.flume.Event;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventHelper;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.Arrays;

public class DemoSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(DemoSink.class);

    private String dirPath;
    private int interval;
    private int maxBytesToLog;

    @Override
    public void configure(Context context) {
        dirPath = context.getString("dirPath");
        dirPath = new File(dirPath).getAbsolutePath();
        interval = context.getInteger("interval", 1);
        maxBytesToLog = context.getInteger("maxBytesToLog", 16);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = this.getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;
        try {
            transaction.begin();
            event = channel.take();
            if (event != null) {
                dealWithEvent(event);
                if (logger.isInfoEnabled()) {
                    logger.info("Event: " + EventHelper.dumpEvent(event, this.maxBytesToLog));
                }
            } else {
                result = Status.BACKOFF;
            }
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
            throw new EventDeliveryException("Failed to event: " + event, e);
        } finally {
            transaction.close();
        }
        return result;
    }

    private void dealWithEvent(Event event) throws Exception {
        byte[] bytes = event.getBody();
        Integer[] shape = string2Ints(event.getHeaders().get("ori shape"));
        if (shape.length != 3 && shape.length != 4) {
            throw new Exception("Data Shape Error.");
        }
        int row = shape[1], col = shape[2];
        File file = new File(dirPath + "/" + System.currentTimeMillis() + ".png");
        ImageIO.write(getImage(bytes, row, col), "png", file);
        Thread.sleep(interval);
    }

    private BufferedImage getImage(byte[] data, int row, int col) {
        BufferedImage image = new BufferedImage(col, row, BufferedImage.TYPE_BYTE_GRAY);
        for (int i = 0; i < row; i++) {
            for (int j = 0; j < col; j++) {
                int gray = (int) data[i * col + j];
                gray += gray >= 0 ? 0 : 256;
                image.setRGB(j, i, new Color(gray, gray, gray, 255).getRGB());
            }
        }
        return image;
    }

    private Integer[] string2Ints(String str) {
        return Arrays.stream(str.substring(1, str.length() - 1).split(","))
                .map(String::trim).map(Integer::parseInt).toArray(Integer[]::new);
    }
}
