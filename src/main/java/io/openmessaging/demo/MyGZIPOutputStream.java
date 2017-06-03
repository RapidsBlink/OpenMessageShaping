package io.openmessaging.demo;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by yche on 6/3/17.
 */
class MyGZIPOutputStream extends GZIPOutputStream {

    public MyGZIPOutputStream(OutputStream out) throws IOException {
        super(out);
    }

    public void setLevel(int level) {
        def.setLevel(level);
    }
}