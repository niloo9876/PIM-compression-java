package org.pim.compression;

import com.upmem.dpu.Dpu;
import com.upmem.dpu.DpuException;
import com.upmem.dpu.DpuSystem;

import java.io.*;

public class Compression {
    private static final int BLOCK_SIZE = 32 * 1024;
    private static final int NR_DPUS = 64;
    private static final int NR_TASKLETS = 24;
    private static final int MASK = 128;
    private static final int TOTAL_NR_TASKLETS = NR_DPUS * NR_TASKLETS;
    private static final String DPU_COMPRESS_PROGRAM = "/home/upmem0016/niloofar_nemone/PIM-compression/snappy/dpu-compress/compress.dpu";
    private static int fileLength;
    private static byte[][] input;
    private static byte[][] output;
    private static byte[][] outputLength;
    private static byte[][] inputLength;
    private static BufferedInputStream reader;
    private static BufferedOutputStream writer;


    public static void snappySetup(String inputFileName) {
        File inputFile = new File(inputFileName);
        File outputFile = new File("output.snappy");
       try {
           reader =  new BufferedInputStream(new FileInputStream(inputFile));
           outputFile.createNewFile();
           writer = new BufferedOutputStream(new FileOutputStream(outputFile));

       } catch (Exception e) {
           System.out.println("Exception occurred while opening the file");
       }

        // Set the input length
        fileLength = (int) inputFile.length();
        outputLength = new byte[NR_DPUS][4 * NR_TASKLETS]; // wram output_length
        inputLength = new byte[NR_DPUS][4]; // wram input_length
        System.out.println(inputLength[0]);
    }

    public static void snappyCompress() throws IOException {

        int numBlocks = (fileLength + BLOCK_SIZE - 1) / BLOCK_SIZE;
        int inputBlocksPerDpu = (numBlocks + NR_DPUS - 1) / NR_DPUS;
        int inputBlocksPerTasklet = (numBlocks + TOTAL_NR_TASKLETS - 1) / TOTAL_NR_TASKLETS;

        int maxLength = DMA_ALIGNED(maxCompressedLength(inputBlocksPerDpu * BLOCK_SIZE));
        System.out.println(maxLength);

        System.out.println(String.format("numBlocks %d, inputBlocksPerDPU %d, inputBlocksPerTasklet %d", numBlocks, inputBlocksPerDpu, inputBlocksPerTasklet));
        // TODO: Not very memory efficient, should be able to allocate buffer for only the dpus that need to be running
        input = new byte[NR_DPUS][inputBlocksPerDpu * BLOCK_SIZE]; // mram input_buffer
        output = new byte[NR_DPUS][maxLength];// mram output_buffer
        byte [][] inputBlockOffset = new byte[NR_DPUS][4 * NR_TASKLETS];
        byte [][] outputOffset = new byte[NR_DPUS][4 * NR_TASKLETS];

        int dpuIndex = 0;
        int taskIndex = 0;
        int dpuBlocks = 0;
        int bytesRead;
        for (int i = 0; i < numBlocks; i++) {
            // we have reached the next DPU's boundary, update the index
            if (dpuBlocks == inputBlocksPerDpu || i == numBlocks-1) {

                // write the input length for each dpu
                write(dpuBlocks * BLOCK_SIZE, inputLength[dpuIndex], 0);
                System.out.println(input[dpuIndex] == null);
                System.out.println((dpuBlocks == 0 ? 1 : dpuBlocks) * BLOCK_SIZE);
                assert (input[dpuIndex] != null);
                bytesRead = reader.read(input[dpuIndex], 0, (dpuBlocks == 0 ? 1 : dpuBlocks) * BLOCK_SIZE);
                // TODO: Handle the scenario where the last block is smaller than block size
                assert (bytesRead == (dpuBlocks == 0 ? 1 : dpuBlocks) * BLOCK_SIZE);
                System.out.println(String.format("DPU %d, numBlocks %d, bytesRead %d", dpuIndex, dpuBlocks, bytesRead));
                dpuIndex++;
                taskIndex = 0;
                dpuBlocks = 0;
            }

            // If we have reached the next tasks's boundary, save the offset
            if (dpuBlocks == (inputBlocksPerTasklet * taskIndex)) {
                // TODO: Potential bug for the first tasklet, first dpuBlock
                // Assign values to inputBlockOffser and outputOffset (L514-515)
                write(i, inputBlockOffset[dpuIndex],4 * taskIndex);
                write(DMA_ALIGNED(maxCompressedLength(BLOCK_SIZE * dpuBlocks)), outputOffset[dpuIndex], 4 * taskIndex);
                System.out.println("output length aligned " + DMA_ALIGNED(maxCompressedLength(BLOCK_SIZE * dpuBlocks)) + " unaligned " + maxCompressedLength(BLOCK_SIZE * dpuBlocks));
                taskIndex++;
            }
            dpuBlocks++;
        }

        // Write the decompressed block size and length (L523-525) to the output file
        writeInt32(fileLength);
        writeInt32(BLOCK_SIZE);

        try(DpuSystem system = DpuSystem.allocate(NR_DPUS, "")) {
            system.load(DPU_COMPRESS_PROGRAM);
            // copy the block size
            byte[] blockSize = new byte[4];
            write(BLOCK_SIZE, blockSize, 0);
            system.copy("block_size", blockSize);
	        System.out.println("copied block size");
            system.copy("input_length", inputLength);
            System.out.println("copied input length");
	        system.copy("input_buffer", input);
            System.out.println("copied input");
	        system.copy("input_block_offset", inputBlockOffset);
            System.out.println("copied input block offset");
	        system.copy("output_offset", outputOffset);
            System.out.println("copied output offset");

            system.exec();
            System.out.println("Executed");
            // Copy out output_buffer and output_length
            system.copy(outputLength, "output_length");
            System.out.println("copied output length");
            system.copy(output, "output_buffer");
            System.out.println("copied output buffer");


            for (int dpu=0; dpu < NR_DPUS; dpu++) {
                for (int task=0; task < NR_TASKLETS; task++) {
                    // Read the size
                    int size = readInt(outputLength[dpu], 4 * task);
                    int offset = readInt(outputOffset[dpu], 4 * task);
                    writer.write(output[dpu], offset, size);
                }
            }
        } catch ( Exception e) {
            System.out.println("Exception occurred:" + e.getMessage());
        }

        // Close all files and readers
        reader.close();
        writer.flush();
        writer.close();

        System.out.println("Done compressing; the result is written to output.txt");
    }

    private static int DMA_ALIGNED(int x) {
        return (x + 7) & ~7;
    }

    /**
     * Given the uncompressed length, returns the maximum possible length when compressed
     * @param uncompressedLength
     * @return
     */
    private static int maxCompressedLength(int uncompressedLength) {
        if (uncompressedLength > 0)
            return (32 + uncompressedLength + uncompressedLength / 6);
        else
            return 0;
    }

    /**
     * Writes data to the array in offset
     * @param data
     * @param buffer
     * @param offset
     */
    private static void write(int data, byte[] buffer, int offset) {
        buffer[offset + 0] = (byte) ((data >>  0) & 0xff);
        buffer[offset + 1] = (byte) ((data >>  8) & 0xff);
        buffer[offset + 2] = (byte) ((data >> 16) & 0xff);
        buffer[offset + 3] = (byte) ((data >> 24) & 0xff);
    }

    private static int readInt(byte[] buffer, int offset) {
        byte b0 = buffer[offset + 0];
        byte b1 = buffer[offset + 1];
        byte b2 = buffer[offset + 2];
        byte b3 = buffer[offset + 3];

        return ((b0 & 0xff) <<  0)
                | ((b1 & 0xff) <<  8)
                | ((b2 & 0xff) << 16)
                | ((b3 & 0xff) << 24);
    }

    private static void writeInt32(int val) throws IOException {
        if (val < (1 << 7)) {
		    writer.write(val);
        }
        else if (val < (1 << 14)) {
            writer.write(val | MASK);
            writer.write(val >> 7);
        }
        else if (val < (1 << 21)) {
            writer.write(val | MASK);
            writer.write((val >> 7) | MASK);
            writer.write(val >> 14);
        }
        else if (val < (1 << 28)) {
            writer.write(val | MASK);
            writer.write((val >> 7) | MASK);
            writer.write((val >> 14) | MASK);
            writer.write(val >> 21);
        }
        else {
            writer.write(val | MASK);
            writer.write((val >> 7) | MASK);
            writer.write((val >> 14) | MASK);
            writer.write((val >> 21) | MASK);
            writer.write(val >> 28);
        }
    }

    public static void main(String[] args) throws DpuException {
        // java -cp .:/usr/share/java/dpu.jar:./target/PIM-compression-java-1.0-SNAPSHOT.jar org.pim.compression.Compression
        // TODO: Add options for other inout files
        String inputFile = "/home/upmem0016/andrada/PIM-compression/test/alice.txt";
        System.out.println("Using input file " + inputFile);

        try {
            snappySetup(inputFile);
            snappyCompress();
        } catch (Exception e) {
            System.out.println("Exception occurred:" + e.getMessage());
            e.printStackTrace();
        }
    }
}
