import org.objectweb.asm.*;
import java.io.*;
import java.nio.file.*;

/**
 * OracleDialect.openJdbcConnection: 创建 OracleConnection 后, 若 Debezium config 配置了
 * connection.pdb.name, 则立即 ALTER SESSION SET CONTAINER 到 PDB,
 * 使 schema 读取/快照在 PDB 上下文(LogMiner 仍由 Debezium 在 CDB 执行)。
 */
public class PatchOracleDialect {
    static final String CLS = "org/apache/seatunnel/connectors/seatunnel/cdc/oracle/source/OracleDialect.class";
    static final String DIALECT = "org/apache/seatunnel/connectors/seatunnel/cdc/oracle/source/OracleDialect";

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
        System.out.println("patched " + CLS);
    }

    static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192]; int n;
        while ((n = in.read(buf)) > 0) bos.write(buf, 0, n);
        return bos.toByteArray();
    }

    static byte[] patch(byte[] classBytes) {
        ClassReader cr = new ClassReader(classBytes);
        ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);
        ClassVisitor cv = new ClassVisitor(Opcodes.ASM9, cw) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
                MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
                if (!name.equals("openJdbcConnection")) return mv;
                return new MethodVisitor(Opcodes.ASM9, mv) {
                    boolean afterCreate = false;
                    boolean injected = false;

                    @Override
                    public void visitMethodInsn(int opcode, String owner, String mname, String mdesc, boolean itf) {
                        super.visitMethodInsn(opcode, owner, mname, mdesc, itf);
                        if (owner.equals("org/apache/seatunnel/connectors/seatunnel/cdc/oracle/utils/OracleConnectionUtils")
                                && mname.equals("createOracleConnection")) {
                            afterCreate = true;
                        }
                    }

                    @Override
                    public void visitInsn(int opcode) {
                        if (afterCreate && !injected && opcode == Opcodes.ARETURN) {
                            inject();
                            injected = true;
                        }
                        super.visitInsn(opcode);
                    }

                    void inject() {
                        System.out.println("  injecting setSessionToPdb into openJdbcConnection...");
                        mv.visitVarInsn(Opcodes.ASTORE, 2);                    // OracleConnection conn
                        mv.visitVarInsn(Opcodes.ALOAD, 1);                     // config
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                                "org/apache/seatunnel/connectors/cdc/base/config/JdbcSourceConfig",
                                "getDbzConnectorConfig",
                                "()Lio/debezium/relational/RelationalDatabaseConnectorConfig;", false);
                        mv.visitTypeInsn(Opcodes.CHECKCAST,
                                "io/debezium/connector/oracle/OracleConnectorConfig");
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                                "io/debezium/connector/oracle/OracleConnectorConfig",
                                "getPdbName", "()Ljava/lang/String;", false);
                        mv.visitVarInsn(Opcodes.ASTORE, 3);                    // String pdb
                        // [DBG] System.out.println("[DCP-DBG] pdb="); System.out.println(pdb);
                        mv.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
                        mv.visitLdcInsn("[DCP-DBG] openJdbcConnection pdb=");
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V", false);
                        mv.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
                        mv.visitVarInsn(Opcodes.ALOAD, 3);
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V", false);
                        Label skip = new Label();
                        mv.visitVarInsn(Opcodes.ALOAD, 3);
                        mv.visitJumpInsn(Opcodes.IFNULL, skip);
                        mv.visitVarInsn(Opcodes.ALOAD, 2);
                        mv.visitVarInsn(Opcodes.ALOAD, 3);
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                                "io/debezium/connector/oracle/OracleConnection",
                                "setSessionToPdb", "(Ljava/lang/String;)V", false);
                        mv.visitLabel(skip);
                        mv.visitFrame(Opcodes.F_NEW, 4,
                                new Object[]{DIALECT,
                                        "org/apache/seatunnel/connectors/cdc/base/config/JdbcSourceConfig",
                                        "io/debezium/connector/oracle/OracleConnection",
                                        "java/lang/String"},
                                0, null);
                        mv.visitVarInsn(Opcodes.ALOAD, 2);
                    }
                };
            }
        };
        cr.accept(cv, 0);
        return cw.toByteArray();
    }
}
