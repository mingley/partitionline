import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

/** Independent five-byte wire header vectors, without codec or registry behavior. */
public final class ConformanceGenericSchemaHeader {
    private record Vector(String name, long id, byte[] payload) {}
    private static List<Vector> vectors() {
        return List.of(new Vector("empty-zero",0,new byte[0]),
            new Vector("empty-max",0xffffffffL,new byte[0]),
            new Vector("one",1,new byte[]{0}),
            new Vector("byte-order",0x01020304L,new byte[]{0,1,2,3,4,(byte)255}),
            new Vector("signed-max",0x7fffffffL,new byte[]{65,66,67}),
            new Vector("unsigned-high",0x80000000L,new byte[]{(byte)255,0,(byte)128}),
            new Vector("max-payload",0xffffffffL,new byte[]{0,0,0,0,0,10,13}));
    }
    private static byte[] encode(Vector v) {
        return ByteBuffer.allocate(5+v.payload.length).put((byte)0).putInt((int)v.id).put(v.payload).array();
    }
    private static String decode(byte[] frame) {
        if (frame.length<5) { return "truncated:"+frame.length; }
        if (frame[0]!=0) { return "bad-magic:"+Byte.toUnsignedInt(frame[0]); }
        long id=Integer.toUnsignedLong(ByteBuffer.wrap(frame,1,4).getInt());
        return "ok:"+id+":"+HexFormat.of().formatHex(Arrays.copyOfRange(frame,5,frame.length));
    }
    public static void main(String[] args) throws Exception {
        if (args.length!=2 || (!args[0].equals("generate") && !args[0].equals("verify"))) {
            throw new IllegalArgumentException("generate|verify directory");
        }
        Path directory=Path.of(args[1]);
        Files.createDirectories(directory);
        int valid=0, invalid=0;
        for (Vector vector:vectors()) {
            byte[] expected=encode(vector);
            if (args[0].equals("generate")) { Files.write(directory.resolve(vector.name+".bin"),expected); }
            byte[] actual=Files.readAllBytes(directory.resolve(vector.name+".bin"));
            if (!Arrays.equals(actual,expected) || !decode(actual).equals("ok:"+vector.id+":"+HexFormat.of().formatHex(vector.payload))) {
                throw new AssertionError("header or payload differs: "+vector.name);
            }
            valid++;
        }
        StringBuilder dispositions=new StringBuilder();
        for (int length=0;length<5;length++) {
            byte[] frame=new byte[length];
            if (length>0) { frame[0]=(byte)255; }
            String name="truncated-"+length;
            if (args[0].equals("generate")) { Files.write(directory.resolve(name+".bin"),frame); }
            byte[] actual=Files.readAllBytes(directory.resolve(name+".bin"));
            if (!decode(actual).equals("truncated:"+length)) { throw new AssertionError("truncation precedence"); }
            dispositions.append(name).append('\t').append(decode(actual)).append('\n'); invalid++;
        }
        for (int magic:new int[]{1,255}) {
            byte[] frame=new byte[]{(byte)magic,1,2,3,4};
            String name="bad-magic-"+magic;
            if (args[0].equals("generate")) { Files.write(directory.resolve(name+".bin"),frame); }
            byte[] actual=Files.readAllBytes(directory.resolve(name+".bin"));
            if (!decode(actual).equals("bad-magic:"+magic)) { throw new AssertionError("bad magic"); }
            dispositions.append(name).append('\t').append(decode(actual)).append('\n'); invalid++;
        }
        if (args[0].equals("generate")) { Files.writeString(directory.resolve("negative-dispositions.tsv"),dispositions); }
        System.out.printf("{\"status\":\"pass\",\"valid\":%d,\"invalid\":%d,\"profile\":\"generic-five-byte-header\"}%n",valid,invalid);
    }
}
