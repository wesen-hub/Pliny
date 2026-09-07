import org.objectweb.asm.*;
import java.io.*;
import java.nio.file.*;

/**
 * OracleSchema.readTableSchema: 在 readSchema 调用前, 若 connectorConfig 配置了
 * database.pdb.name, 主动 ALTER SESSION SET CONTAINER 到 PDB。
 * 覆盖所有调用方(含 SplitFetcher 的 registerDatabaseHistory 路径, 其连接不走 openJdbcConnection)。
 */
public class PatchOracleSchemaPdb {
    static final String CLS = "org/apache/seatunnel/connectors/seatunnel/cdc/oracle/utils/OracleSchema.class";
    static final String SCHEMA = "org/apache/seatunnel/connectors/seatunnel/cdc/oracle/utils/OracleSchema";

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
        ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        ClassVisitor cv = new ClassVisitor(Opcodes.ASM9, cw) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
                MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
                if (!name.equals("readTableSchema")) return mv;
                return new MethodVisitor(Opcodes.ASM9, mv) {
                    boolean injected = false;
                    @Override
                    public void visitMethodInsn(int opcode, String owner, String mname, String mdesc, boolean itf) {
                        if (!injected && owner.equals("io/debezium/connector/oracle/OracleConnection")
                                && mname.equals("readSchema")) {
                            inject();
                            injected = true;
                        }
                        super.visitMethodInsn(opcode, owner, mname, mdesc, itf);
                    }
                    void inject() {
                        System.out.println("  injecting setSessionToPdb into readTableSchema...");
                        // String pdb = this.connectorConfig.getPdbName();
                        mv.visitVarInsn(Opcodes.ALOAD, 0);
                        mv.visitFieldInsn(Opcodes.GETFIELD, SCHEMA, "connectorConfig",
                                "Lio/debezium/connector/oracle/OracleConnectorConfig;");
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "io/debezium/connector/oracle/OracleConnectorConfig",
                                "getPdbName", "()Ljava/lang/String;", false);
                        mv.visitVarInsn(Opcodes.ASTORE, 9);
                        // if (pdb != null) conn.setSessionToPdb(pdb);
                        Label skip = new Label();
                        mv.visitVarInsn(Opcodes.ALOAD, 9);
                        mv.visitJumpInsn(Opcodes.IFNULL, skip);
                        mv.visitVarInsn(Opcodes.ALOAD, 3);   // conn (checkcast 后的 OracleConnection)
                        mv.visitVarInsn(Opcodes.ALOAD, 9);
                        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "io/debezium/connector/oracle/OracleConnection",
                                "setSessionToPdb", "(Ljava/lang/String;)V", false);
                        mv.visitLabel(skip);
                    }
                };
            }
        };
        cr.accept(cv, 0);
        return cw.toByteArray();
    }
}
