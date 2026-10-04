import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * connector-cdc-base 位点透传补丁(设计方案 §3.1, 路径 1 位点即序号)。
 *
 * <p>改动(仅重打 base 一个 jar):
 * <ol>
 *   <li>重写 {@code SeaTunnelRowDebeziumDeserializeSchema$Builder.build()}: 在把
 *       metadataConverters 传给构造器前调用
 *       {@code PositionMetadataConverter.merge(...)}, null → [位点, 快照标志, before 行];
 *       非 null → 缺失项尾部追加。2.3.12 各源从未调用 setMetadataConverters(null 装配),
 *       本补丁同时修复该装配缺口。</li>
 *   <li>插入式加宽 {@code IncrementalSource.getProducedCatalogTables()}: 非
 *       compatible 分支 {@code getfield catalogTables} 后套
 *       {@code ProducedTypeWidener.widen(...)}, 把 producedType 的每个 CatalogTable
 *       物理列末尾追加 __src_pos / __snapshot / __src_before 三列 —— 与 deserialize 行尾追加的
 *       metadata 列对齐, 否则引擎 SeaTunnelSourceCollector 按 producedType 计算行
 *       字节数必然越界(ArrayIndexOutOfBoundsException)。</li>
 *   <li>向 jar 追加四个类文件: PositionMetadataConverter / SnapshotFlagMetadataConverter
 *       / BeforeImageMetadataConverter / ProducedTypeWidener(与注入点的 invokestatic 引用同包同名)。</li>
 * </ol>
 *
 * <p>效果: ROW 格式行末多出三列 __src_pos(位点字符串)/ __snapshot(快照标志)/
 * __src_before(before 行 JSON), 下游 CdcSeqTransform 按源类型编码 seq = f(位点),
 * 透传 snapshot 到 CdcRecord, 并从 before 行 JSON 提取 beforeKey 做主键修改检测。
 *
 * <p>用法(与 PatchOracle* 工艺一致):
 * <pre>
 *   javac -cp asm-9.5.jar -d classes/ cdc-patch-src/org/apache/seatunnel/connectors/cdc/debezium/*.java
 *   javac -cp .:asm-9.5.jar PatchCdcBasePosition.java
 *   java  -cp .:asm-9.5.jar PatchCdcBasePosition connector-cdc-base-2.3.12.jar classes/
 * </pre>
 * 幂等性: 重复打补丁时 build() 已被重写 → 检测到 merge 引用则跳过; getProducedCatalogTables
 * 已注入 widen 则跳过; 两类检测独立(旧补丁 jar 可增量补 widen);
 * 新类文件重复追加按覆盖处理(zip 后项覆盖前项)。
 */
public class PatchCdcBasePosition {

    static final String BUILDER = "org/apache/seatunnel/connectors/cdc/debezium/row/SeaTunnelRowDebeziumDeserializeSchema$Builder";
    static final String SCHEMA = "org/apache/seatunnel/connectors/cdc/debezium/row/SeaTunnelRowDebeziumDeserializeSchema";
    static final String MERGE_OWNER = "org/apache/seatunnel/connectors/cdc/debezium/PositionMetadataConverter";
    static final String WIDENER_OWNER = "org/apache/seatunnel/connectors/cdc/debezium/ProducedTypeWidener";
    static final String INCREMENTAL_SOURCE = "org/apache/seatunnel/connectors/cdc/base/source/IncrementalSource";
    static final String METADATA_CONV = "[Lorg/apache/seatunnel/connectors/cdc/debezium/MetadataConverter;";
    static final String INIT_DESC = "([Lorg/apache/seatunnel/connectors/cdc/debezium/MetadataConverter;"
            + "Ljava/util/List;Ljava/time/ZoneId;"
            + "Lorg/apache/seatunnel/connectors/cdc/debezium/DebeziumDeserializationConverterFactory;"
            + "Lorg/apache/seatunnel/connectors/cdc/base/schema/SchemaChangeResolver;Ljava/util/Map;)V";

    static final String[] EXTRA_CLASSES = {"PositionMetadataConverter", "SnapshotFlagMetadataConverter",
            "BeforeImageMetadataConverter", "ProducedTypeWidener"};

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("usage: PatchCdcBasePosition <jar> <classes-dir>");
            System.exit(1);
        }
        String jarPath = args[0];
        Path classesDir = Paths.get(args[1]);
        Map<String, byte[]> extra = new LinkedHashMap<>();
        for (String n : EXTRA_CLASSES) {
            String clsPath = "org/apache/seatunnel/connectors/cdc/debezium/" + n + ".class";
            Path p = classesDir.resolve(clsPath);
            if (!Files.exists(p)) {
                System.out.println("MISSING CLASS " + p);
                System.exit(3);
            }
            extra.put(clsPath, Files.readAllBytes(p));
        }

        Path tmp = Paths.get(jarPath + ".tmp");
        boolean hit = false;
        boolean alreadyPatched = false;
        boolean hitProducedType = false;
        java.util.Set<String> existing = new java.util.HashSet<>();
        try (java.util.zip.ZipFile zin = new java.util.zip.ZipFile(jarPath);
             java.util.zip.ZipOutputStream zout = new java.util.zip.ZipOutputStream(new FileOutputStream(tmp.toFile()))) {
            for (java.util.Enumeration<? extends java.util.zip.ZipEntry> e = zin.entries(); e.hasMoreElements(); ) {
                java.util.zip.ZipEntry entry = e.nextElement();
                existing.add(entry.getName());
                byte[] data = readAll(zin.getInputStream(entry));
                if (entry.getName().equals(BUILDER + ".class")) {
                    byte[] patched = patchBuilder(data);
                    if (patched == null) {
                        alreadyPatched = true;
                        System.out.println("  builder.build() 已含 merge 注入, 跳过(幂等)");
                    } else {
                        data = patched;
                        hit = true;
                        System.out.println("  builder.build() 已重写: metadataConverters → PositionMetadataConverter.merge(...)");
                    }
                }
                if (entry.getName().equals(INCREMENTAL_SOURCE + ".class")) {
                    byte[] patched = patchProducedType(data);
                    if (patched == null) {
                        System.out.println("  getProducedCatalogTables() 已含 widen 注入, 跳过(幂等)");
                    } else {
                        data = patched;
                        hitProducedType = true;
                        System.out.println("  getProducedCatalogTables() 已注入: catalogTables → ProducedTypeWidener.widen(...)");
                    }
                }
                java.util.zip.ZipEntry ne = new java.util.zip.ZipEntry(entry.getName());
                ne.setTime(entry.getTime());
                zout.putNextEntry(ne);
                zout.write(data);
                zout.closeEntry();
            }
            for (Map.Entry<String, byte[]> x : extra.entrySet()) {
                if (existing.contains(x.getKey())) {
                    System.out.println("  = " + x.getKey() + "(已存在, 跳过)");
                    continue;
                }
                java.util.zip.ZipEntry ne = new java.util.zip.ZipEntry(x.getKey());
                ne.setTime(System.currentTimeMillis());
                zout.putNextEntry(ne);
                zout.write(x.getValue());
                zout.closeEntry();
                System.out.println("  + " + x.getKey());
            }
        }
        if (!hit && !alreadyPatched) {
            System.out.println("NO MATCH: " + BUILDER);
            System.exit(2);
        }
        Files.move(tmp, Paths.get(jarPath), StandardCopyOption.REPLACE_EXISTING);
        System.out.println("patched: " + jarPath);
    }

    static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) {
            bos.write(buf, 0, n);
        }
        return bos.toByteArray();
    }

    /**
     * 插入式加宽 getProducedCatalogTables(): 非 compatible 分支的
     * {@code getfield catalogTables} 紧跟 ARETURN 处套 widen()。
     * 不重写方法体(避免引入分支导致 StackMapTable 失效), 只在原指令流中插入一次
     * invokestatic; 已含 widen 注入时返回 null(幂等跳过)。
     */
    static byte[] patchProducedType(byte[] classBytes) {
        final boolean[] already = {false};
        ClassReader probeReader = new ClassReader(classBytes);
        probeReader.accept(new ClassVisitor(Opcodes.ASM9) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
                if ("getProducedCatalogTables".equals(name) && "()Ljava/util/List;".equals(desc)) {
                    return new MethodVisitor(Opcodes.ASM9) {
                        @Override
                        public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean itf) {
                            if (opcode == Opcodes.INVOKESTATIC && WIDENER_OWNER.equals(owner) && "widen".equals(name)) {
                                already[0] = true;
                            }
                        }
                    };
                }
                return null;
            }
        }, 0);
        if (already[0]) {
            return null;
        }
        final boolean[] found = {false};
        ClassReader cr = new ClassReader(classBytes);
        ClassWriter cw2 = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);
        cr.accept(new ClassVisitor(Opcodes.ASM9, cw2) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
                if ("getProducedCatalogTables".equals(name) && "()Ljava/util/List;".equals(desc)) {
                    found[0] = true;
                    MethodVisitor mv = cw2.visitMethod(access, name, desc, signature, exceptions);
                    return new MethodVisitor(Opcodes.ASM9, mv) {
                        private boolean lastWasCatalogTables = false;

                        @Override
                        public void visitFieldInsn(int opcode, String owner, String name2, String desc2) {
                            super.visitFieldInsn(opcode, owner, name2, desc2);
                            lastWasCatalogTables = opcode == Opcodes.GETFIELD
                                    && INCREMENTAL_SOURCE.equals(owner) && "catalogTables".equals(name2);
                        }

                        @Override
                        public void visitInsn(int opcode) {
                            if (opcode == Opcodes.ARETURN && lastWasCatalogTables) {
                                super.visitMethodInsn(Opcodes.INVOKESTATIC, WIDENER_OWNER, "widen",
                                        "(Ljava/util/List;)Ljava/util/List;", false);
                            }
                            lastWasCatalogTables = false;
                            super.visitInsn(opcode);
                        }
                    };
                }
                return super.visitMethod(access, name, desc, signature, exceptions);
            }
        }, 0);
        if (!found[0]) {
            return null;
        }
        return cw2.toByteArray();
    }

    /** 重写 build() 方法体; 已含 merge 注入时返回 null(幂等跳过) */
    static byte[] patchBuilder(byte[] classBytes) {
        final boolean[] already = {false};
        // 先只读扫描: 是否已注入 merge(幂等)
        ClassReader probeReader = new ClassReader(classBytes);
        probeReader.accept(new ClassVisitor(Opcodes.ASM9) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
                if ("build".equals(name) && ("()L" + SCHEMA + ";").equals(desc)) {
                    return new MethodVisitor(Opcodes.ASM9) {
                        @Override
                        public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean itf) {
                            if (opcode == Opcodes.INVOKESTATIC && MERGE_OWNER.equals(owner) && "merge".equals(name)) {
                                already[0] = true;
                            }
                        }
                    };
                }
                return null;
            }
        }, 0);
        if (already[0]) {
            return null; // 已打过, 幂等跳过
        }
        // 重写 build(): metadataConverters 传参前套 merge()
        final boolean[] found = {false};
        ClassReader cr = new ClassReader(classBytes);
        ClassWriter cw2 = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);
        cr.accept(new ClassVisitor(Opcodes.ASM9, cw2) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
                if ("build".equals(name) && ("()L" + SCHEMA + ";").equals(desc)) {
                    found[0] = true;
                    MethodVisitor mv = cw2.visitMethod(access, name, desc, signature, exceptions);
                    mv.visitCode();
                    mv.visitTypeInsn(Opcodes.NEW, SCHEMA);
                    mv.visitInsn(Opcodes.DUP);
                    mv.visitVarInsn(Opcodes.ALOAD, 0);
                    mv.visitFieldInsn(Opcodes.GETFIELD, BUILDER, "metadataConverters", METADATA_CONV);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, MERGE_OWNER, "merge",
                            "(" + METADATA_CONV + ")" + METADATA_CONV, false);
                    mv.visitVarInsn(Opcodes.ALOAD, 0);
                    mv.visitFieldInsn(Opcodes.GETFIELD, BUILDER, "tables", "Ljava/util/List;");
                    mv.visitVarInsn(Opcodes.ALOAD, 0);
                    mv.visitFieldInsn(Opcodes.GETFIELD, BUILDER, "serverTimeZone", "Ljava/time/ZoneId;");
                    mv.visitVarInsn(Opcodes.ALOAD, 0);
                    mv.visitFieldInsn(Opcodes.GETFIELD, BUILDER, "userDefinedConverterFactory",
                            "Lorg/apache/seatunnel/connectors/cdc/debezium/DebeziumDeserializationConverterFactory;");
                    mv.visitVarInsn(Opcodes.ALOAD, 0);
                    mv.visitFieldInsn(Opcodes.GETFIELD, BUILDER, "schemaChangeResolver",
                            "Lorg/apache/seatunnel/connectors/cdc/base/schema/SchemaChangeResolver;");
                    mv.visitVarInsn(Opcodes.ALOAD, 0);
                    mv.visitFieldInsn(Opcodes.GETFIELD, BUILDER, "tableIdTableChangeMap", "Ljava/util/Map;");
                    mv.visitMethodInsn(Opcodes.INVOKESPECIAL, SCHEMA, "<init>", INIT_DESC, false);
                    mv.visitInsn(Opcodes.ARETURN);
                    mv.visitMaxs(0, 0);
                    mv.visitEnd();
                    return null; // 原方法体由返回 null 忽略
                }
                return super.visitMethod(access, name, desc, signature, exceptions);
            }
        }, 0);
        if (!found[0]) {
            return null; // 未找到方法(理论不会)
        }
        return cw2.toByteArray();
    }
}
