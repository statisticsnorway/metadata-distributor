package no.ssb.dapla.metadata.distributor.parquet;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class NIOPathBasedInputFile implements InputFile {

    final Path path;

    public NIOPathBasedInputFile(Path path) {
        Objects.requireNonNull(path);
        if (!Files.isRegularFile(path)) {
            throw new RuntimeException("Not a regular file: " + path.toString());
        }
        if (!Files.isReadable(path)) {
            throw new RuntimeException("File cannot be read: " + path.toString());
        }
        this.path = path;
    }

    @Override
    public long getLength() throws IOException {
        return Files.size(path);
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new NIOPathBasedSeekableInputStream(path);
    }

    @Override
    public String toString() {
        return NIOPathBasedInputFile.class.getSimpleName() + "[" + path.toString() + "]";
    }

    static class NIOPathBasedSeekableInputStream extends SeekableInputStream {
        final byte[] minibuf = new byte[1];
        final ByteBuffer minibb = ByteBuffer.wrap(minibuf);
        final SeekableByteChannel channel;

        NIOPathBasedSeekableInputStream(Path path) {
            try {
                channel = Files.newByteChannel(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public void seek(long p) throws IOException {
            channel.position(p);
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
            int n = channel.read(byteBuffer);
            return n;
        }

        @Override
        public void readFully(ByteBuffer byteBuffer) throws IOException {
            int n = channel.read(byteBuffer);
            while (byteBuffer.hasRemaining() && n != -1) {
                // TODO determine whether we need to add a little sleep time to avoid excess CPU usage
                n = channel.read(byteBuffer);
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
