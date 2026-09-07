import org.objectweb.asm.*;
import java.io.*;
import java.nio.file.*;

/** 版本查询: BANNER_FULL(多行,正则不匹配) 改为单行 BANNER 列;LIKE 改 'Oracle%' */
public class PatchOracleVersion2 {
    static final String CLS = "io/debezium/connector/oracle/OracleConnection.class";
    public static void main(String[] args) throws Exception {
        String jarPath = args[0];
        Path tmp = Paths.get(jarPath + ".tmp");
        boolean hit = false;
        try (java.util.zip.ZipFile zin = new java.util.zip.ZipFile(jarPath);
             java.util.zip.ZipOutputStream zout = new java.util.zip.ZipOutputStream(new FileOutputStream(tmp.toFile()))) {
            for (java.util.Enumeration<? extends java.util.zip.ZipEntry> e = zin.entries(); e.hasMoreElements(); ) {
                java.util.zip.ZipEntry entry = e.nextElement();
                byte[] data = readAll(zin.getInputStream(entry));
                if (entry.getName().equals(CLS)) { data = patch(data); hit = true; }
                java.util.zip.ZipEntry ne = new java.util.zip.ZipEntry(entry.getName());
                ne.setTime(entry.getTime());
                zout.putNextEntry(ne);
                zout.write(data);
                zout.closeEntry();
            }
        }
        if (!hit) { System.out.println("NO MATCH"); System.exit(2); }
        Files.move(tmp, Paths.get(jarPath), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        System.out.println("patched");
    }
    static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192]; int n;
        while ((n = in.read(buf)) > 0) bos.write(buf, 0, n);
        return bos.toByteArray();
    }
    static byte[] patch(byte[] classBytes) {
        ClassReader cr = new ClassReader(classBytes);
        ClassWriter cw = new ClassWriter(cr, 0);
        ClassVisitor cv = new ClassVisitor(Opcodes.ASM9, cw) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
                MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
                return new MethodVisitor(Opcodes.ASM9, mv) {
                    @Override
                    public void visitLdcInsn(Object value) {
                        if (value instanceof String) {
                            String s = (String) value;
                            if (s.startsWith("SELECT BANNER_FULL FROM V$VERSION")) {
                                System.out.println("  rewrite: " + s);
                                s = "SELECT BANNER FROM V$VERSION WHERE BANNER LIKE 'Oracle%'";
                            } else if (s.contains("LIKE 'Oracle Database%'")) {
                                System.out.println("  rewrite: " + s);
                                s = s.replace("LIKE 'Oracle Database%'", "LIKE 'Oracle%'");
                            }
                            value = s;
                        }
                        super.visitLdcInsn(value);
                    }
                };
            }
        };
        cr.accept(cv, 0);
        return cw.toByteArray();
    }
}
