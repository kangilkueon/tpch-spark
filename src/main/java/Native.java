package csdvirt;

import java.nio.ByteBuffer;

public class Native {
    public static native long csdvirt_create_reader(String path);
    public static native int csdvirt_next_batch(long riscvReaderPtr, ByteBuffer buffer, int max_size);
    public static native int csdvirt_release_reader(long riscvReaderPtr);
}