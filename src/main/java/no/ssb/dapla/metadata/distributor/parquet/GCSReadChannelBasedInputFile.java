package no.ssb.dapla.metadata.distributor.parquet;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

public class GCSReadChannelBasedInputFile implements InputFile {
    private final Blob blob;

    public GCSReadChannelBasedInputFile(Blob blob) {
        this.blob = blob;
    }

    @Override
    public long getLength() throws IOException {
        return blob.getSize();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new GCSReadChannelSeekableInputStream(blob);
    }

    @Override
    public String toString() {
        return "GCSReadChannelBasedInputFile[" + blob.getBlobId().toString() + "]";
    }

    static class GCSReadChannelSeekableInputStream extends SeekableInputStream {
        long pos = 0;
        final byte[] minibuf = new byte[1];
        final ByteBuffer minibb = ByteBuffer.wrap(minibuf);
        final ReadChannel readChannel;

        GCSReadChannelSeekableInputStream(Blob blob) {
            readChannel = blob.reader();
        }

        @Override
        public long getPos() throws IOException {
            return pos;
        }

        @Override
        public void seek(long p) throws IOException {
            readChannel.seek(p);
            pos = p;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(ByteBuffer.wrap(bytes));
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            readFully(ByteBuffer.wrap(bytes, start, len));
        }

        @Override
        public int read(ByteBuffer byteBuffer) throws IOException {
            int n = readChannel.read(byteBuffer);
            if (n > 0) {
                pos += n;
            }
            return n;
        }

        @Override
        public void readFully(ByteBuffer byteBuffer) throws IOException {
            int n = readChannel.read(byteBuffer);
            if (n > 0) {
                pos += n;
            }
            while (byteBuffer.hasRemaining() && n != -1) {
                // TODO determine whether we need to add a little sleep time to avoid excess CPU usage
                n = readChannel.read(byteBuffer);
                if (n > 0) {
                    pos += n;
                }
            }
        }

        @Override
        public int read() throws IOException {
            minibb.clear();
            readFully(minibb);
            if (minibb.position() == 0) {
                return -1;
            }
            return minibuf[0] & 255;
        }
    }
}