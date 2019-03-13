package top.akiyamamio.flume.source.demo;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractPollableSource;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class DemoPollableSource extends AbstractPollableSource {

    private int id = 0;

    private String filePath;
    private int maxRead;
    private int batchSize;
    private int interval;

    private BufferedInputStream inputStream;
    private byte[] toSend;
    private HashMap<String, String> header;

    @Override
    protected void doConfigure(Context context) throws FlumeException {
        filePath = context.getString("filePath");
        maxRead = context.getInteger("maxRead", 100);
        batchSize = context.getInteger("batchSize", 10);
        interval = context.getInteger("interval", 10);
    }

    @Override
    protected void doStart() throws FlumeException {
        try {
            inputStream = new BufferedInputStream(new FileInputStream(filePath));
            int ndim = readInt(inputStream) - 2048;
            int singleByteSize = 1;
            int[] shape = new int[ndim];
            for (int i = 0; i < ndim; i++) {
                shape[i] = readInt(inputStream);
                singleByteSize *= (i > 0 ? shape[i] : 1);
            }
            toSend = new byte[singleByteSize];
            header = new HashMap<>();
            header.put("ori shape", Arrays.toString(shape));
        } catch (Exception e) {
            e.printStackTrace();
            throw new FlumeException(e);
        }
    }

    @Override
    protected void doStop() throws FlumeException {
        try {
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new FlumeException(e);
        }
    }

    @Override
    protected Status doProcess() throws EventDeliveryException {
        try {
            if (id >= maxRead) return Status.BACKOFF;
            ArrayList<Event> list = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                if (inputStream.read(toSend) == -1)
                    throw new IOException("Error reading file.");
                list.add(EventBuilder.withBody(toSend, header));
                if (++id >= batchSize) break;
            }
            this.getChannelProcessor().processEventBatch(list);
            Thread.sleep(interval * batchSize);
            return Status.READY;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            throw new EventDeliveryException(e);
        }
    }

    private static int readInt(InputStream inputStream) throws Exception {
        byte[] intReader = new byte[4];
        int size = inputStream.read(intReader), res = 0;
        if (size != 4) throw new Exception("File format error.");
        for (int i = 0; i < 4; i++) {
            int toAdd = (int) intReader[i] + (intReader[i] >= 0 ? 0 : 256);
            res = (res << 8) + toAdd;
        }
        return res;
    }
}
