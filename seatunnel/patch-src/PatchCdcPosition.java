import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

/**
 * ① 位点透传补丁: 给 SeaTunnel 2.3.12 CDC connector 接线官方
 * {@code PositionMetadataConverter.INSTANCE}(read 输出 "type:k1:v1..." 位点字符串)。
 *
 * <p>两种注入模式:
 * <pre>
 *   builder:    {@code createDebeziumDeserializationSchema} 内 Builder.build() 前注入
 *               {@code .setMetadataConverters([PositionMetadataConverter.INSTANCE, SnapshotFlagMetadataConverter.INSTANCE])}
 *               行尾两列: 位点字符串 + 快照标志(mysql / postgres / oracle / sqlserver)
 *   mongo-emit: 重写 MongoDBConnectorDeserializationSchema.emit, 开头调用
 *               {@code PositionAppender.append(row, record)} 追加行尾两列(mongodb 自研反序列化链, 无 Builder)
 * </pre>
 *
 * <p>用法:
 * <pre>
 *   java PatchCdcPosition &lt;jar&gt; builder    org/.../MySqlIncrementalSource
 *   java PatchCdcPosition &lt;jar&gt; mongo-emit org/.../MongoDBConnectorDeserializationSchema
 * </pre>
 * 原 jar 备份为 &lt;jar&gt;.bak(已存在则不覆盖); 改写失败(NO MATCH)时退出码 2 且不动原 jar。
 */
public class PatchCdcPosition {

    static final String BUILDER_CLS =
            "org/apache/seatunnel/connectors/cdc/debezium/row/SeaTunnelRowDebeziumDeserializeSchema$Builder";
    static final String CONVERTER_CLS =
            "org/apache/seatunnel/connectors/cdc/debezium/PositionMetadataConverter";
    static final String SNAPSHOT_CONVERTER_CLS =
            "org/apache/seatunnel/connectors/cdc/debezium/SnapshotFlagMetadataConverter";
    static final String META_CONVERTER_CLS =
            "org/apache/seatunnel/connectors/cdc/debezium/MetadataConverter";
    static final String APPENDER_CLS = "com/dcp/seatunnel/cdc/PositionAppender";
    static final String SEATUNNEL_ROW = "org/apache/seatunnel/api/table/type/SeaTunnelRow";
    static final String SOURCE_RECORD = "org/apache/kafka/connect/source/SourceRecord";
    static final String COLLECTOR = "org/apache/seatunnel/api/source/Collector";

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("用法: java PatchCdcPosition <jar> <builder|mongo-emit> <目标类内部名>");
            System.exit(1);
        }
        String jarPath = args[0];
        String mode = args[1];
        String target = args[2];
        byte[] targetClass = patchClass(readEntry(jarPath, target + ".class"), mode, target);
        rewriteJar(jarPath, target + ".class", targetClass);
        System.out.println("patched " + target + " [" + mode + "]");
    }

    static byte[] readEntry(String jarPath, String entry) throws Exception {
        try (ZipFile zin = new ZipFile(jarPath)) {
            ZipEntry ze = zin.getEntry(entry);
            if (ze == null) {
                System.err.println("NO MATCH: 类不存在 " + entry);
                System.exit(2);
            }
            try (InputStream in = zin.getInputStream(ze)) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) > 0) {
                    bos.write(buf, 0, n);
                }
                return bos.toByteArray();
            }
        }
    }

    static void rewriteJar(String jarPath, String entry, byte[] data) throws Exception {
        Path backup = Paths.get(jarPath + ".bak");
        if (!Files.exists(backup)) {
            Files.copy(Paths.get(jarPath), backup);
        }
        Path tmp = Paths.get(jarPath + ".tmp");
        try (ZipFile zin = new ZipFile(jarPath);
             ZipOutputStream zout = new ZipOutputStream(new FileOutputStream(tmp.toFile()))) {
            for (java.util.Enumeration<? extends ZipEntry> e = zin.entries(); e.hasMoreElements(); ) {
                ZipEntry ze = e.nextElement();
                byte[] d = ze.getName().equals(entry) ? data : readAll(zin.getInputStream(ze));
                ZipEntry ne = new ZipEntry(ze.getName());
                ne.setTime(ze.getTime());
                zout.putNextEntry(ne);
                zout.write(d);
                zout.closeEntry();
            }
        }
        Files.move(tmp, Paths.get(jarPath), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    static byte[] readAll(InputStream in) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
            bos.write(buf, 0, n);
        }
        return bos.toByteArray();
    }

    static byte[] patchClass(byte[] classBytes, String mode, String target) {
        ClassReader cr = new ClassReader(classBytes);
        // COMPUTE_MAXS: 注入不改变控制流(不新增跳转目标), 原 StackMapTable 帧仍合法,
        // 但需重算 max_stack(注入的 dup/anewarray 序列会加深栈)
        ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);
        ClassVisitor cv = new ClassVisitor(Opcodes.ASM9, cw) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String desc,
                                             String signature, String[] exceptions) {
                MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
                if ("builder".equals(mode)) {
                    return new BuilderPatch(mv);
                }
                // mongo-emit: 重写 emit(SourceRecord, SeaTunnelRow, Collector) 方法体
                if ("mongo-emit".equals(mode) && name.equals("emit")
                        && desc.equals("(L" + SOURCE_RECORD + ";L" + SEATUNNEL_ROW
                                + ";L" + COLLECTOR + ";)V")) {
                    System.out.println("  rewrite emit: " + target + "#emit");
                    return new EmitRewrite(mv);
                }
                return mv;
            }
        };
        cr.accept(cv, 0);
        return cw.toByteArray();
    }

    /**
     * builder 模式: 在 Builder.build() 调用前注入
     * {@code setMetadataConverters([PositionMetadataConverter.INSTANCE, SnapshotFlagMetadataConverter.INSTANCE])}
     * —— 行尾两列: __src_pos(位点字符串) + __snapshot(Boolean 快照标志),
     * 与 CdcSeqTransform {@code values[names.length] / values[names.length+1]} 读取对齐。
     */
    static class BuilderPatch extends MethodVisitor {
        BuilderPatch(MethodVisitor mv) {
            super(Opcodes.ASM9, mv);
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc,
                                    boolean itf) {
            if (opcode == Opcodes.INVOKEVIRTUAL && owner.equals(BUILDER_CLS)
                    && name.equals("build")) {
                System.out.println("  inject setMetadataConverters([pos, snapshot]) before Builder.build()");
                // 栈顶为 Builder: dup 保留链式引用
                super.visitInsn(Opcodes.DUP);
                // new MetadataConverter[]{ PositionMetadataConverter.INSTANCE, SnapshotFlagMetadataConverter.INSTANCE }
                super.visitInsn(Opcodes.ICONST_2);
                super.visitTypeInsn(Opcodes.ANEWARRAY, META_CONVERTER_CLS);
                super.visitInsn(Opcodes.DUP);
                super.visitInsn(Opcodes.ICONST_0);
                super.visitFieldInsn(Opcodes.GETSTATIC, CONVERTER_CLS, "INSTANCE",
                        "L" + CONVERTER_CLS + ";");
                super.visitInsn(Opcodes.AASTORE);
                super.visitInsn(Opcodes.DUP);
                super.visitInsn(Opcodes.ICONST_1);
                super.visitFieldInsn(Opcodes.GETSTATIC, SNAPSHOT_CONVERTER_CLS, "INSTANCE",
                        "L" + SNAPSHOT_CONVERTER_CLS + ";");
                super.visitInsn(Opcodes.AASTORE);
                super.visitMethodInsn(Opcodes.INVOKEVIRTUAL, BUILDER_CLS, "setMetadataConverters",
                        "([L" + META_CONVERTER_CLS + ";)L" + BUILDER_CLS + ";", false);
            }
            super.visitMethodInsn(opcode, owner, name, desc, itf);
        }
    }

    /**
     * mongo-emit 方法体重写: 首次 visitCode 时写入新体, 之后吞掉原方法全部指令事件。
     * (不能返回 null —— ASM 中 null 会丢弃整个方法)
     */
    static class EmitRewrite extends MethodVisitor {
        private boolean replaced;

        EmitRewrite(MethodVisitor mv) {
            super(Opcodes.ASM9, mv);
        }

        @Override
        public void visitCode() {
            if (replaced) {
                return;
            }
            replaced = true;
            super.visitCode();
            // row = PositionAppender.append(row, record)   (行尾两列: 位点 + 快照标志)
            super.visitVarInsn(Opcodes.ALOAD, 2);
            super.visitVarInsn(Opcodes.ALOAD, 1);
            super.visitMethodInsn(Opcodes.INVOKESTATIC, APPENDER_CLS, "append",
                    "(L" + SEATUNNEL_ROW + ";L" + SOURCE_RECORD + ";)L" + SEATUNNEL_ROW + ";", false);
            super.visitVarInsn(Opcodes.ASTORE, 2);
            // collector.collect(row)
            super.visitVarInsn(Opcodes.ALOAD, 3);
            super.visitVarInsn(Opcodes.ALOAD, 2);
            super.visitMethodInsn(Opcodes.INVOKEINTERFACE, COLLECTOR, "collect",
                    "(Ljava/lang/Object;)V", true);
            super.visitInsn(Opcodes.RETURN);
        }

        // 原方法体指令全部忽略(新体已在 visitCode 写入)
        @Override public void visitInsn(int opcode) { }
        @Override public void visitVarInsn(int opcode, int varIndex) { }
        @Override public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) { }
        @Override public void visitTypeInsn(int opcode, String type) { }
        @Override public void visitFieldInsn(int opcode, String owner, String name, String desc) { }
        @Override public void visitJumpInsn(int opcode, org.objectweb.asm.Label label) { }
        @Override public void visitLabel(org.objectweb.asm.Label label) { }
        @Override public void visitLdcInsn(Object value) { }
        @Override public void visitIincInsn(int varIndex, int increment) { }
        @Override public void visitIntInsn(int opcode, int operand) { }
        @Override public void visitMultiANewArrayInsn(String desc, int dims) { }
        @Override public void visitLookupSwitchInsn(org.objectweb.asm.Label dflt, int[] keys, org.objectweb.asm.Label[] labels) { }
        @Override public void visitTableSwitchInsn(int min, int max, org.objectweb.asm.Label dflt, org.objectweb.asm.Label... labels) { }
        @Override public void visitTryCatchBlock(org.objectweb.asm.Label start, org.objectweb.asm.Label end,
                                                 org.objectweb.asm.Label handler, String type) { }
        @Override public void visitLocalVariable(String name, String desc, String signature,
                                                 org.objectweb.asm.Label start, org.objectweb.asm.Label end, int index) { }
        @Override public void visitLineNumber(int line, org.objectweb.asm.Label start) { }
        @Override public void visitMaxs(int maxStack, int maxLocals) {
            super.visitMaxs(0, 0); // COMPUTE_MAXS 重算
        }
    }
}
