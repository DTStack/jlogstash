package com.dtstack.jlogstash.filters;

import com.dtstack.jlogstash.annotation.Required;
import com.dtstack.jlogstash.exception.InitializeException;
import com.dtstack.jlogstash.render.FreeMarkerRender;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.*;
import java.io.*;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

/**
 * 动态执行java code
 *
 * @author zxb
 * @version 1.0.0
 *          2017年03月24日 16:25
 * @since Jdk1.6
 */
public class Java extends BaseFilter {

    private final static String className = "JavaCodeUtil";

    private final static String packageName = "com.dtstack.jlogstash.filters";

    private final static String templateName = "JavaCodeTemplate";

    private final static String methodName = "filter";

    private final static String templatePath = "code.ftl";
    private static String userDir = System.getProperty("user.dir");
    private static File classPathDir;

    private static Logger logger = LoggerFactory.getLogger(Java.class);

    @Required(required = true)
    private static String code;

    private static String template;

    private static Object target;

    private static Class<? extends Object> targetClass;

    private static Method targetMethod;

    static {
        if (StringUtils.isEmpty(template)) {
            try {
                template = initTemplate();
            } catch (IOException e) {
                throw new InitializeException("init template error", e);
            }
        }

        // 最后一个File.separator必要加上，否则ClassLoader从此目录加载类时将加载不到。
        String classPath = userDir + File.separator + "plugin" + File.separator + "filter" + File.separator + "classes" + File.separator;
        classPathDir = new File(classPath);
        if (!classPathDir.exists()) {
            classPathDir.mkdirs();
        }
    }

    private FreeMarkerRender freeMarkerRender;


    public Java(Map config) {
        super(config);
    }

    private static String initTemplate() throws IOException {
        InputStream inputStream = Java.class.getClassLoader().getResourceAsStream(templatePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuilder stringBuilder = new StringBuilder();
        String line = null;
        try {
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line);
            }
            return stringBuilder.toString();
        } finally {
            try {
                bufferedReader.close();
            } catch (IOException e) {
                logger.error("close template reader error", e);
            }
        }
    }

    public void prepare() {
        if (targetMethod == null) {
            compileCode();
        }
    }

    protected Map filter(Map event) {
        try {
            return (Map) targetMethod.invoke(target, event);
        } catch (Exception e) {
            logger.error("invoke Java filter error", e);
        }
        return null;
    }

    /**
     * 编译code中代码，获取JavaCodeUtil对象
     */
    private void compileCode() {
        String sourceCode = renderCode();
        JavaFileObject file = new JavaSourceFromString(className, sourceCode);
        Iterable<? extends JavaFileObject> fileObjects = Arrays.asList(file);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();

        List<String> options = new ArrayList<String>();
        options.add("-d");
        options.add(classPathDir.getAbsolutePath());
        options.add("-classpath");
        options.add(getClassPath());

        JavaCompiler.CompilationTask task = compiler.getTask(null, null, diagnostics, options, null, fileObjects);
        if (task.call()) {
            try {
                // 使用Java.class的ClassLoader，反射调用addURL方法添加编译后的class输出路径到该classLoader的classpath中。
                URLClassLoader urlClassLoader = (URLClassLoader) Java.class.getClassLoader();
                Class<?> clClazz = urlClassLoader.getClass();
                Method method = clClazz.getDeclaredMethod("addURL", URL.class);
                method.setAccessible(true);
                method.invoke(urlClassLoader, classPathDir.toURI().toURL());

                // 使用Java.class的ClassLoader加载该类。
                // 使用此类加载器，既可以访问父类加载器AppClassLoader的类成员，也可以访问当前Java这个类的成员。
                String fullClassName = packageName + "." + className;
                targetClass = urlClassLoader.loadClass(fullClassName);
                target = targetClass.newInstance();
                targetMethod = targetClass.getMethod(methodName, Map.class);
            } catch (Exception e) {
                throw new InitializeException("find class error", e);
            } finally {
                deleteFile(classPathDir);
            }
        } else {
            deleteFile(classPathDir);

            StringBuilder sb = new StringBuilder();
            for (Diagnostic diagnostic : diagnostics.getDiagnostics()) {
                sb.append(diagnostic.getMessage(Locale.CHINA));
            }
            throw new InitializeException(sb.toString());
        }
    }

    /**
     * 从模板中渲染JavaCodeUtil源码
     *
     * @return
     */
    private String renderCode() {
        try {
            freeMarkerRender = new FreeMarkerRender(template, templateName);
        } catch (IOException e) {
            throw new InitializeException("init template error", e);
        }

        Map<String, String> context = new HashMap<String, String>();
        context.put("code", code);
        return freeMarkerRender.render(context);
    }

    /**
     * 获取当前类的classpath
     *
     * @return
     */
    private String getClassPath() {
        URLClassLoader currentCL = (URLClassLoader) getClass().getClassLoader();
        URLClassLoader parentCL = (URLClassLoader) getClass().getClassLoader().getParent();
        StringBuilder sb = new StringBuilder();
        sb.append(getClassPathURL(parentCL)); // 父加载器（Logstash）
        sb.append(getClassPathURL(currentCL)); // 类加载器（当前Filter的）
        sb.append(classPathDir.getAbsolutePath());
        return sb.toString();
    }

    /**
     * 获取当前类加载器所加载的jar包路径
     *
     * @param classLoader
     * @return
     */
    private String getClassPathURL(URLClassLoader classLoader) {
        StringBuilder sb = new StringBuilder();
        for (URL url : classLoader.getURLs()) {
            sb.append(url.getFile()).append(File.pathSeparator);
        }
        return sb.toString();
    }

    /**
     * 删除文件或目录
     * @param file
     */
    private void deleteFile(File file) {
        if (file.exists()) {
            if (file.isFile()) {
                file.delete();
            } else if (file.isDirectory()) {
                File[] files = file.listFiles();
                for (int i = 0, len = files.length; i < len; i++) {
                    deleteFile(files[i]);
                }
                file.delete(); // delete the directory
            }
        }
    }
}

class JavaSourceFromString extends SimpleJavaFileObject {
    final String code;

    JavaSourceFromString(String name, String code) {
        super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
        this.code = code;
    }

    @Override
    public CharSequence getCharContent(boolean ignoreEncodingErrors) {
        return code;
    }
}