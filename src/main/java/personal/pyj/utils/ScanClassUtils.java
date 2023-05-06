package personal.pyj.utils;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * scan class utils
 * @author zx065
 */
public class ScanClassUtils {

    /**
     * get classes
     * @param pack 包
     * @return 包下所有类
     */
    public static  Set<Class<?>> getClasses(String pack) {
        Set<Class<?>> classes = new LinkedHashSet<>();
        // get package name and replace to url
        String packageDirName = pack.replace(".", "/");
        try {
            Enumeration<URL> dirs = Thread.currentThread().getContextClassLoader().getResources(packageDirName);
            while (dirs.hasMoreElements()) {
                // get next element
                URL url = dirs.nextElement();
                String protocol = url.getProtocol();
                // if it is saved at server like a file
                if ("file".equals(protocol)) {
                    String file = url.getFile();
                    String decode = URLDecoder.decode(file, "UTF-8");
                    findClassesInPackageByFile(pack, decode, classes);
                } else if ("jar".equals(protocol)) {
                    // 如果是jar包文件
                    // 定义一个JarFile
                    System.out.println("jar类型的扫描");
                    try( JarFile jar = ((JarURLConnection) url.openConnection()).getJarFile()) {
                        // 从此jar包 得到一个枚举类
                        Enumeration<JarEntry> entries = jar.entries();
                        findClassesInPackageByJar(entries, packageDirName, classes);
                    } catch (IOException e) {
                        // log.error("在扫描用户定义视图时从jar包获取文件出错");
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return classes;
    }

    private static void findClassesInPackageByFile(String packageName, String packagePath, Set<Class<?>> classes){
        File dir = new File(packagePath);

        if(!dir.exists() || !dir.isDirectory()){
            return;
        }
        // 获取目录下的目录和以.class结尾的文件
        File[] dirFiles = dir.listFiles(file -> file.isDirectory() || file.getName().endsWith(".class"));
        if (dirFiles == null) return;
        for (File dirFile : dirFiles) {
            if(dirFile.isDirectory()) {
                // 目录继续往里找
                findClassesInPackageByFile(packageName + "." + dirFile.getName(), dirFile.getAbsolutePath(), classes);
            } else {
                // 去除.class
                String className = packageName + "." + dirFile.getName().substring(0, dirFile.getName().length() - 6);
                // 添加class
                try {
                    classes.add(Thread.currentThread().getContextClassLoader().loadClass(className));
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void findClassesInPackageByJar(Enumeration<JarEntry> entries, String packageDirName, Set<Class<?>> classes) {

        // 同样的进行循环迭代
        while (entries.hasMoreElements()) {
            // 获取jar里的一个实体 可以是目录 和一些jar包里的其他文件 如META-INF等文件
            JarEntry entry = entries.nextElement();
            String name = entry.getName();
            // 如果是以/开头的
            if (name.charAt(0) == '/') {
                // 获取后面的字符串
                name = name.substring(1);
            }
            // 如果前半部分和定义的包名相同
            if (name.startsWith(packageDirName)) {
                // 如果是一个.class文件 而且不是目录
                if (name.endsWith(".class") && !entry.isDirectory()) {
                    String className = entry.getName().replace("/", ".").substring(0, entry.getName().length() - 6);
                    try {
                        // 添加到classes
                        classes.add(Class.forName(className));
                    } catch (ClassNotFoundException e) {
                        // .error("添加用户自定义视图类错误 找不到此类的.class文件");
                        e.printStackTrace();
                    } catch (NoClassDefFoundError e) {
                        // log.error("添加用户自定义视图类错误 找不到此类的.class文件");
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}