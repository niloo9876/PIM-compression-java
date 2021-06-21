package org.pim.compression;

import com.upmem.dpu.Dpu;
import com.upmem.dpu.DpuException;
import com.upmem.dpu.DpuSystem;

import java.io.File;

public class Compression {
    public void readInputHost(String inputFile) {
        File uncompressed = new File(inputFile);

        // TODO Set the input length
        uncompressed.length();
    }

    public static void main(String[] args) throws DpuException {
        int blockSize = 32 * 1024;
        String inputFile = "./test/alice.txt";

        // TODO: Add options for other inout files
        System.out.println("Using input file " + inputFile);

        // TODO: Remove the code below
        try(DpuSystem system = DpuSystem.allocate(1, "")) {
            Dpu dpu = system.dpus().get(0);

            byte[] data = new byte[] {0, 1, 2, 3, 4, 5, 6, 7};
            byte[] result = new byte[8];

            dpu.load("java_example.dpu");
            dpu.copy("variable", data);
            dpu.exec(System.out);
            dpu.copy(result, "variable");

            long value =
                    ((result[0] & 0xffl) << (8 * 0)) |
                            ((result[1] & 0xffl) << (8 * 1)) |
                            ((result[2] & 0xffl) << (8 * 2)) |
                            ((result[3] & 0xffl) << (8 * 3)) |
                            ((result[4] & 0xffl) << (8 * 4)) |
                            ((result[5] & 0xffl) << (8 * 5)) |
                            ((result[6] & 0xffl) << (8 * 6)) |
                            ((result[7] & 0xffl) << (8 * 7));

            System.out.println(String.format("Variable after = 0x%016x", value));
        }
    }
}
