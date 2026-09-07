import org.objectweb.asm.*;
import java.io.*;
import java.nio.file.*;

/**
 * 给 SeaTunnel OracleSourceConfigFactory 打补丁:
 * 在 database.dbname 配置之后注入:
 *   if (databaseList.size() > 1)
 *       props.setProperty("connection.pdb.name", (String) databaseList.get(1));
 * 使 Debezium Oracle connector 连接 CDB 后 ALTER SESSION SET CONTAINER 到 PDB,
 * 从而在 CDB 里跑 LogMiner 的同时能看到 PDB 的表结构。
 */
public class PatchOraclePdb {
    static final String CLS = "org/apache/seatunnel/connectors/seatunnel/cdc/oracle/config/OracleSourceConfigFactory.class";
    static final String INTERNAL = "org/apache/seatunnel/connectors/seatunnel/cdc/oracle/config/OracleSourceConfigFactory";

    public static void main(String[] args) throws Exception {
        String jarPath = args[0];
        Path tmp = Paths.get(jarPath + ".tmp");
        try (java.util.zip.ZipFile zin = new java.util.zip.ZipFile(jarPath);
             java.util.zip.ZipOutputStream zout = new java.util.zip.ZipOutputStream(new FileOutputStream(tmp.toFile()))) {
            for (java.util.Enumeration<? extends java.util.zip.ZipEntry> e = zin.entries(); e.hasMoreElements(); ) {
                java.util.zip.ZipEntry entry = e.nextElement();
                byte[] data = readAll(zin.getInputStream(entry));
                if (entry.getName().equals(CLS)) {
                    data = patch(data);
                    System.out.println("patched " + CLS);
                }
                java.util.zip.ZipEntry ne = new java.util.zip.ZipEntry(entry.getName());
                ne.setTime(entry.getTime());
                zout.putNextEntry(ne);
                zout.write(data);
                zout.closeEntry();
            }
        }
        Files.move(tmp, Paths.get(jarPath), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        System.out.println("done");
    }

    static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) bos.write(buf, 0, n);
        return bos.toByteArray();
    }

    static byte[] patch(byte[] classBytes) {
        ClassReader cr = new ClassReader(classBytes);
        ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        ClassVisitor cv = new ClassVisitor(Opcodes.ASM9, cw) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
                MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
                return new MethodVisitor(Opcodes.ASM9, mv) {
                    boolean pendingInject = false;
                    boolean injected = false;

                    @Override
                    public void visitLdcInsn(Object value) {
                        super.visitLdcInsn(value);
                        if (value instanceof String && "database.history".equals(value)) {
                            pendingInject = true;
                        }
                    }

                    @Override
                    public void visitMethodInsn(int opcode, String owner, String mname, String mdesc, boolean itf) {
                        if (!injected && pendingInject
                                && owner.equals("java/util/Properties") && mname.equals("setProperty")) {
                            injectPdb();
                            injected = true;
                        }
                        super.visitMethodInsn(opcode, owner, mname, mdesc, itf);
                        if (mname.equals("setProperty")) {
                            pendingInject = false;
                        }
                    }

                    void injectPdb() {
                        System.out.println("  injecting pdb config...");
                        mv.visitVarInsn(Opcodes.ALOAD, 0);                       // this
                        mv.visitFieldInsn(Opcodes.GETFIELD, INTERNAL, "databaseList", "Ljava/util/List;");
                        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "size", "()I", true);
                        mv.visitInsn(Opcodes.ICONST_1);
                        Label skip = new Label();
                        mv.visitJumpInsn(Opcodes.IF_ICMPLE, skip);              // size<=1 跳过
                        mv.visitVarInsn(Opcodes.ALOAD, 2);                       // props
                        mv.visitLdcInsn("database.pdb.name");
                        mv.visitVarInsn(Opcodes.ALOAD, 0);                       // this
                        mv.visitFieldInsn(Opcodes.GETFIELD, INTERNAL, "databaseList", "Ljava/util/List;");
                        mv.visitInsn(Opcodes.ICONST_1);
                        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
                        mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/String");
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/util/Properties", "setProperty", "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/Object;", false);
                        mv.visitInsn(Opcodes.POP);
                        mv.visitLabel(skip);
                    }
                };
            }
        };
        cr.accept(cv, 0);
        return cw.toByteArray();
    }
}
