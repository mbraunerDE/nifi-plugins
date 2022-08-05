package de.mbrauner.nifiplugins.processors.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.apache.nifi.processor.io.OutputStreamCallback;

public class StringStreamCallback implements OutputStreamCallback {

    private String text;

    public StringStreamCallback(String text) {
        this.text = text;
    }

    @Override public void process(OutputStream out) throws IOException {
        if (Optional.ofNullable(text).isPresent()) {
            try (OutputStreamWriter osw = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                osw.write(text);
                osw.flush();
            }
        } else {
            throw new NullPointerException("Never write a null-valued-string to an output stream!");
        }
    }
}

