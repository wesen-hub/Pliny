package com.dcp.worker;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

/**
 * 白名单 ClassLoader(方案 §8.2):业务 jar 的装载器。
 *
 * <p>加载策略:
 * <ul>
 *   <li><b>child-first</b>:白名单包前缀(业务代码 + 二级 SPI 契约)优先从业务 jar 加载,
 *       业务 jar 可携带与宿主并存的类而不冲突;</li>
 *   <li><b>parent-first</b>:其余一切(一级 SPI 契约 com.dcp.scheduler.*、slf4j、
 *       kafka-clients、jackson、spring-jdbc)走宿主类加载器 —— 契约与三方依赖由宿主
 *       统一提供,版本对齐(worker pom 显式声明与业务 jar 同版本的三方依赖)。</li>
 * </ul>
 */
public class WhitelistClassLoader extends URLClassLoader {

    /** 白名单包前缀(child-first)。业务 jar 各自前缀由装配期传入。 */
    public static final List<String> DEFAULT_CHILD_FIRST_PREFIXES = Arrays.asList(
            "com.dcp.synclink.",
            "com.dcp.indexer.",
            "com.dcp.outlet.rt.",
            "com.dcp.outlet.sdk.");

    private final List<String> childFirstPrefixes;

    public WhitelistClassLoader(URL[] urls, ClassLoader parent) {
        this(urls, parent, DEFAULT_CHILD_FIRST_PREFIXES);
    }

    public WhitelistClassLoader(URL[] urls, ClassLoader parent, List<String> childFirstPrefixes) {
        super(urls, parent);
        this.childFirstPrefixes = childFirstPrefixes;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loaded = findLoadedClass(name);
            if (loaded == null && isChildFirst(name)) {
                // child-first:先尝试本 loader(业务 jar 内),未命中再委托 parent
                try {
                    loaded = findClass(name);
                } catch (ClassNotFoundException ignored) {
                    // 落到 parent(如白名单包内引用宿主提供的契约/三方类)
                }
            }
            if (loaded == null) {
                loaded = super.loadClass(name, false);
            }
            if (resolve) {
                resolveClass(loaded);
            }
            return loaded;
        }
    }

    private boolean isChildFirst(String name) {
        for (String prefix : childFirstPrefixes) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
