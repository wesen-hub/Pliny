import org.objectweb.asm.*;
import java.io.*;
import java.nio.file.*;

/**
 * RestJobExecutionEnvironment.getJobConfigParser: isStartWithSavePoint=true 但
 * getLatestCheckpointData 返回空(无可用 checkpoint/savepoint, 如 S3/MinIO 位点被删)时,
 * 2.3.12 原实现静默降级 initial 全量重采(不抛错) → backend CdcStService.startOne 的
 * "restore 失败(无可用 savepoint?)→ 回退 initial 全量重采 → 换新 jobId" 分支永远不触发。
 * 本 patch 注入: isStartWithSavePoint && list 为空 → 抛 IllegalArgumentException
 * (REST 层 ExceptionHandlingFilter 转 HTTP 400 + error body), 使 backend 回退分支生效。
 *
 * 语义依据(2.3.12 CheckpointManager.clearCheckpointIfNeed): 优雅 stop(CANCELED/FINISHED)
 * 会删 job checkpoint 目录; savepoint 停止或进程崩溃则保留。故 restore 找不到状态
 * 必然是"位点已丢失", 回退 initial 是正确的。
 */
public class PatchSavepointGuard {
    static final String CLS = "org/apache/seatunnel/engine/server/rest/RestJobExecutionEnvironment.class";
    static final String ENV = "org/apache/seatunnel/engine/server/rest/RestJobExecutionEnvironment";
    static final String MTP = "org/apache/seatunnel/engine/core/parse/MultipleTableJobConfigParser";

    public static void main(String[] args) throws Exception {
        String jarPath = args[0];
        Path tmp = Paths.get(jarPath + ".tmp");
        boolean hit = false;
        try (java.util.zip.ZipFile zin = new java.util.zip.ZipFile(jarPath);
             java.util.zip.ZipOutputStream zout = new java.util.zip.ZipOutputStream(new FileOutputStream(tmp.toFile()))) {
            for (java.util.Enumeration<? extends java.util.zip.ZipEntry> e = zin.entries(); e.hasMoreElements(); ) {
                java.util.zip.ZipEntry entry = e.nextElement();
                byte[] data = readAll(zin.getInputStream(entry));
                if (entry.getName().equals(CLS)) { data = patch(data, jarPath); hit = true; }
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

    static byte[] patch(byte[] classBytes, String jarPath) throws Exception {
        // COMPUTE_FRAMES: 仅对本类重算帧, 类型层级用被 patch 的 jar 自身解析
        // (fat jar 含 hazelcast/shade 等全部依赖), 解析不到回退 Object 不影响本类
        final java.net.URLClassLoader jarLoader = new java.net.URLClassLoader(
                new java.net.URL[]{new File(jarPath).toURI().toURL()}, ClassLoader.getSystemClassLoader());
        ClassReader cr = new ClassReader(classBytes);
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES) {
            @Override
            protected String getCommonSuperClass(String type1, String type2) {
                if (type1.equals(type2)) return type1;
                try {
                    Class<?> c1 = load(type1), c2 = load(type2);
                    if (c1.isAssignableFrom(c2)) return type1;
                    if (c2.isAssignableFrom(c1)) return type2;
                    if (c1.isInterface() || c2.isInterface()) return "java/lang/Object";
                    Class<?> c = c1;
                    do { c = c.getSuperclass(); } while (c != null && !c.isAssignableFrom(c2));
                    return c == null ? "java/lang/Object" : c.getName().replace('.', '/');
                } catch (Exception e) {
                    return "java/lang/Object";
                }
            }
            Class<?> load(String t) throws Exception {
                return Class.forName(t.replace('/', '.'), false, jarLoader);
            }
        };
        ClassVisitor cv = new ClassVisitor(Opcodes.ASM9, cw) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
                MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
                if (!name.equals("getJobConfigParser")) return mv;
                return new MethodVisitor(Opcodes.ASM9, mv) {
                    boolean injected = false;

                    @Override
                    public void visitTypeInsn(int opcode, String type) {
                        if (!injected && opcode == Opcodes.NEW && type.equals(MTP)) {
                            inject();
                            injected = true;
                        }
                        super.visitTypeInsn(opcode, type);
                    }

                    // 在 new MultipleTableJobConfigParser 之前注入:
                    //   if (isStartWithSavePoint && list1.isEmpty())
                    //       throw new IllegalArgumentException("restore failed: ...");
                    void inject() {
                        System.out.println("  injecting savepoint guard into getJobConfigParser...");
                        Label ok = new Label();
                        mv.visitVarInsn(Opcodes.ALOAD, 0);
                        mv.visitFieldInsn(Opcodes.GETFIELD, ENV, "isStartWithSavePoint", "Z");
                        mv.visitJumpInsn(Opcodes.IFEQ, ok);
                        mv.visitVarInsn(Opcodes.ALOAD, 1);
                        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "isEmpty", "()Z", true);
                        mv.visitJumpInsn(Opcodes.IFEQ, ok);
                        mv.visitTypeInsn(Opcodes.NEW, "java/lang/IllegalArgumentException");
                        mv.visitInsn(Opcodes.DUP);
                        mv.visitLdcInsn("restore failed: start with savepoint but no checkpoint state found for this job, fallback to initial full re-sync");
                        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/IllegalArgumentException", "<init>", "(Ljava/lang/String;)V", false);
                        mv.visitInsn(Opcodes.ATHROW);
                        mv.visitLabel(ok);
                    }
                };
            }
        };
        cr.accept(cv, ClassReader.SKIP_FRAMES);
        return cw.toByteArray();
    }
}
